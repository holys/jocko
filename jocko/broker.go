package jocko

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/jocko/fsm"
	"github.com/travisjeffery/jocko/jocko/metadata"
	"github.com/travisjeffery/jocko/jocko/structs"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
)

const (
	serfLANSnapshot   = "serf/local.snapshot"
	raftState         = "raft/"
	raftLogCacheSize  = 512
	snapshotsRetained = 2
)

var (
	ErrTopicExists     = errors.New("topic exists already")
	ErrInvalidArgument = errors.New("no logger set")
)

// Broker represents a broker in a Jocko cluster, like a broker in a Kafka cluster.
type Broker struct {
	sync.RWMutex
	logger log.Logger
	config *config.BrokerConfig

	// readyForConsistentReads is used to track when the leader server is
	// ready to serve consistent reads, after it has applied its initial
	// barrier. This is updated atomically.
	readyForConsistentReads int32
	// brokerLookup tracks servers in the local datacenter.
	brokerLookup  *brokerLookup
	replicaLookup *replicaLookup
	// The raft instance is used among Jocko brokers within the DC to protect operations that require strong consistency.
	raft          *raft.Raft
	raftStore     *raftboltdb.BoltStore
	raftTransport *raft.NetworkTransport
	raftInmem     *raft.InmemStore
	// raftNotifyCh ensures we get reliable leader transition notifications from the raft layer.
	raftNotifyCh <-chan bool
	// reconcileCh is used to pass events from the serf handler to the raft leader to update its state.
	reconcileCh chan serf.Member
	serf        *serf.Serf
	fsm         *fsm.FSM
	eventChLAN  chan serf.Event

	tracer opentracing.Tracer

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

// New is used to instantiate a new broker.
func NewBroker(config *config.BrokerConfig, tracer opentracing.Tracer, logger log.Logger) (*Broker, error) {
	b := &Broker{
		config:        config,
		logger:        logger.With(log.Int32("id", config.ID), log.String("raft addr", config.RaftAddr)),
		shutdownCh:    make(chan struct{}),
		eventChLAN:    make(chan serf.Event, 256),
		brokerLookup:  NewBrokerLookup(),
		replicaLookup: NewReplicaLookup(),
		reconcileCh:   make(chan serf.Member, 32),
		tracer:        tracer,
	}

	if b.logger == nil {
		return nil, ErrInvalidArgument
	}

	b.logger.Info("hello")

	if err := b.setupRaft(); err != nil {
		b.Shutdown()
		return nil, fmt.Errorf("failed to start raft: %v", err)
	}

	var err error
	b.serf, err = b.setupSerf(config.SerfLANConfig, b.eventChLAN, serfLANSnapshot)
	if err != nil {
		return nil, err
	}

	go b.lanEventHandler()

	go b.monitorLeadership()

	return b, nil
}

// Broker API.

// Run starts a loop to handle requests send back responses.
func (b *Broker) Run(ctx context.Context, requestc <-chan Request, responsec chan<- Response) {
	var conn io.ReadWriter
	var header *protocol.RequestHeader
	var resp protocol.ResponseBody

	for {
		select {
		case request := <-requestc:
			conn = request.Conn
			header = request.Header

			switch req := request.Request.(type) {
			case *protocol.APIVersionsRequest:
				resp = b.handleAPIVersions(header, req)
			case *protocol.ProduceRequest:
				resp = b.handleProduce(header, req)
			case *protocol.FetchRequest:
				resp = b.handleFetch(header, req)
			case *protocol.OffsetsRequest:
				resp = b.handleOffsets(header, req)
			case *protocol.MetadataRequest:
				resp = b.handleMetadata(header, req)
			case *protocol.CreateTopicRequests:
				resp = b.handleCreateTopic(header, req)
			case *protocol.DeleteTopicsRequest:
				resp = b.handleDeleteTopics(header, req)
			case *protocol.LeaderAndISRRequest:
				resp = b.handleLeaderAndISR(header, req)
			}
		case <-ctx.Done():
			return
		}

		responsec <- Response{Conn: conn, Header: header, Response: &protocol.Response{
			CorrelationID: header.CorrelationID,
			Body:          resp,
		}}
	}
}

// Join is used to have the broker join the gossip ring.
// The given address should be another broker listening on the Serf address.
func (b *Broker) JoinLAN(addrs ...string) protocol.Error {
	if _, err := b.serf.Join(addrs, true); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	return protocol.ErrNone
}

// Request handling.

var (
	APIVersions = &protocol.APIVersionsResponse{
		APIVersions: []protocol.APIVersion{
			{APIKey: protocol.ProduceKey, MinVersion: 2, MaxVersion: 2},
			{APIKey: protocol.FetchKey},
			{APIKey: protocol.OffsetsKey},
			{APIKey: protocol.MetadataKey},
			{APIKey: protocol.LeaderAndISRKey},
			{APIKey: protocol.StopReplicaKey},
			{APIKey: protocol.GroupCoordinatorKey},
			{APIKey: protocol.JoinGroupKey},
			{APIKey: protocol.HeartbeatKey},
			{APIKey: protocol.LeaveGroupKey},
			{APIKey: protocol.SyncGroupKey},
			{APIKey: protocol.DescribeGroupsKey},
			{APIKey: protocol.ListGroupsKey},
			{APIKey: protocol.APIVersionsKey},
			{APIKey: protocol.CreateTopicsKey},
			{APIKey: protocol.DeleteTopicsKey},
		},
	}
)

func (b *Broker) handleAPIVersions(header *protocol.RequestHeader, req *protocol.APIVersionsRequest) *protocol.APIVersionsResponse {
	return APIVersions
}

func (b *Broker) handleCreateTopic(header *protocol.RequestHeader, reqs *protocol.CreateTopicRequests) *protocol.CreateTopicsResponse {
	resp := new(protocol.CreateTopicsResponse)
	resp.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Requests))
	isController := b.isController()
	for i, req := range reqs.Requests {
		if !isController {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrNotController.Code(),
			}
			continue
		}
		if req.ReplicationFactor > int16(len(b.LANMembers())) {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrInvalidReplicationFactor.Code(),
			}
			continue
		}
		err := b.createTopic(req.Topic, req.NumPartitions, req.ReplicationFactor)
		resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
			Topic:     req.Topic,
			ErrorCode: err.Code(),
		}
	}
	return resp
}

func (b *Broker) handleDeleteTopics(header *protocol.RequestHeader, reqs *protocol.DeleteTopicsRequest) *protocol.DeleteTopicsResponse {
	resp := new(protocol.DeleteTopicsResponse)
	resp.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Topics))
	isController := b.isController()
	for i, topic := range reqs.Topics {
		if !isController {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     topic,
				ErrorCode: protocol.ErrNotController.Code(),
			}
			continue
		}
		// TODO: this will delete from fsm -- need to delete associated partitions, etc.
		_, err := b.raftApply(structs.DeregisterTopicRequestType, structs.DeregisterTopicRequest{
			structs.Topic{
				Topic: topic,
			},
		})
		if err != nil {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     topic,
				ErrorCode: protocol.ErrUnknown.Code(),
			}
			continue
		}
		resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
			Topic:     topic,
			ErrorCode: protocol.ErrNone.Code(),
		}
	}
	return resp
}

func (b *Broker) handleLeaderAndISR(header *protocol.RequestHeader, req *protocol.LeaderAndISRRequest) *protocol.LeaderAndISRResponse {
	resp := &protocol.LeaderAndISRResponse{
		Partitions: make([]*protocol.LeaderAndISRPartition, len(req.PartitionStates)),
	}
	setErr := func(i int, p *protocol.PartitionState, err protocol.Error) {
		resp.Partitions[i] = &protocol.LeaderAndISRPartition{
			ErrorCode: err.Code(),
			Partition: p.Partition,
			Topic:     p.Topic,
		}
	}
	for i, p := range req.PartitionStates {
		replica, err := b.replicaLookup.Replica(p.Topic, p.Partition)
		isNew := err != nil
		if err != nil {
			replica = &Replica{
				BrokerID: b.config.ID,
				Partition: structs.Partition{
					ID:              p.Partition,
					Partition:       p.Partition,
					Topic:           p.Topic,
					ISR:             p.ISR,
					AR:              p.Replicas,
					ControllerEpoch: p.ZKVersion,
					LeaderEpoch:     p.LeaderEpoch,
					Leader:          p.Leader,
				},
				IsLocal: true,
			}
			b.replicaLookup.AddReplica(replica)
		}
		if err := b.startReplica(replica); err != protocol.ErrNone {
			setErr(i, p, err)
			continue
		}
		if p.Leader == b.config.ID && (replica.Partition.Leader != b.config.ID || isNew) {
			// is command asking this broker to be the new leader for p and this broker is not already the leader for
			if err := b.becomeLeader(replica, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		} else if contains(p.Replicas, b.config.ID) && (!contains(replica.Partition.AR, p.Leader) || isNew) {
			// is command asking this broker to follow leader who it isn't a leader of already
			if err := b.becomeFollower(replica, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		}
		resp.Partitions[i] = &protocol.LeaderAndISRPartition{Partition: p.Partition, Topic: p.Topic, ErrorCode: protocol.ErrNone.Code()}
	}
	return resp
}

func (b *Broker) handleOffsets(header *protocol.RequestHeader, req *protocol.OffsetsRequest) *protocol.OffsetsResponse {
	oResp := new(protocol.OffsetsResponse)
	oResp.Responses = make([]*protocol.OffsetResponse, len(req.Topics))
	for i, t := range req.Topics {
		oResp.Responses[i] = new(protocol.OffsetResponse)
		oResp.Responses[i].Topic = t.Topic
		oResp.Responses[i].PartitionResponses = make([]*protocol.PartitionResponse, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			pResp := new(protocol.PartitionResponse)
			pResp.Partition = p.Partition
			replica, err := b.replicaLookup.Replica(t.Topic, p.Partition)
			if err != nil {
				// TODO: have replica lookup return an error with a code
				pResp.ErrorCode = protocol.ErrUnknown.Code()
				continue
			}
			var offset int64
			if p.Timestamp == -2 {
				offset = replica.Log.OldestOffset()
			} else {
				offset = replica.Log.NewestOffset()
			}
			pResp.Offsets = []int64{offset}
			oResp.Responses[i].PartitionResponses = append(oResp.Responses[i].PartitionResponses, pResp)
		}
	}
	return oResp
}

func (b *Broker) handleProduce(header *protocol.RequestHeader, req *protocol.ProduceRequest) *protocol.ProduceResponses {
	resp := new(protocol.ProduceResponses)
	resp.Responses = make([]*protocol.ProduceResponse, len(req.TopicData))
	for i, td := range req.TopicData {
		presps := make([]*protocol.ProducePartitionResponse, len(td.Data))
		for j, p := range td.Data {
			presp := &protocol.ProducePartitionResponse{}
			state := b.fsm.State()
			_, t, err := state.GetTopic(td.Topic)
			if err != nil {
				presp.ErrorCode = protocol.ErrUnknown.WithErr(err).Code()
				presps[j] = presp
				continue
			}
			if t == nil {
				presp.ErrorCode = protocol.ErrUnknownTopicOrPartition.Code()
				presps[j] = presp
				continue
			}
			replica, err := b.replicaLookup.Replica(td.Topic, p.Partition)
			if err != nil {
				b.logger.Error("produce to partition failed", log.Error("error", err))
				presp.Partition = p.Partition
				presp.ErrorCode = protocol.ErrReplicaNotAvailable.Code()
				presps[j] = presp
				continue
			}
			offset, appendErr := replica.Log.Append(p.RecordSet)
			if appendErr != nil {
				b.logger.Error("commitlog/append failed", log.Error("error", err))
				presp.ErrorCode = protocol.ErrUnknown.Code()
				presps[j] = presp
				continue
			}
			presp.Partition = p.Partition
			presp.BaseOffset = offset
			presp.Timestamp = time.Now().Unix()
			presps[j] = presp
		}
		resp.Responses[i] = &protocol.ProduceResponse{
			Topic:              td.Topic,
			PartitionResponses: presps,
		}
	}
	return resp
}

func (b *Broker) handleMetadata(header *protocol.RequestHeader, req *protocol.MetadataRequest) *protocol.MetadataResponse {
	state := b.fsm.State()
	brokers := make([]*protocol.Broker, 0, len(b.LANMembers()))
	for _, mem := range b.LANMembers() {
		m, ok := metadata.IsBroker(mem)
		if !ok {
			continue
		}
		// TODO: replace this -- just use the addr
		host, portStr, err := net.SplitHostPort(m.BrokerAddr)
		if err != nil {
			panic(err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			panic(err)
		}
		brokers = append(brokers, &protocol.Broker{
			NodeID: b.config.ID,
			Host:   host,
			Port:   int32(port),
		})
	}
	var topicMetadata []*protocol.TopicMetadata
	topicMetadataFn := func(topic *structs.Topic, err protocol.Error) *protocol.TopicMetadata {
		if err != protocol.ErrNone {
			return &protocol.TopicMetadata{
				TopicErrorCode: err.Code(),
				Topic:          topic.Topic,
			}
		}
		partitionMetadata := make([]*protocol.PartitionMetadata, 0, len(topic.Partitions))
		for id := range topic.Partitions {
			_, p, err := state.GetPartition(topic.Topic, id)
			if err != nil {
				partitionMetadata = append(partitionMetadata, &protocol.PartitionMetadata{
					ParititionID:       id,
					PartitionErrorCode: protocol.ErrUnknown.Code(),
				})
				continue
			}
			if p == nil {
				partitionMetadata = append(partitionMetadata, &protocol.PartitionMetadata{
					ParititionID:       id,
					PartitionErrorCode: protocol.ErrUnknownTopicOrPartition.Code(),
				})
				continue
			}
			partitionMetadata = append(partitionMetadata, &protocol.PartitionMetadata{
				ParititionID:       p.ID,
				PartitionErrorCode: protocol.ErrNone.Code(),
				Leader:             p.Leader,
				Replicas:           p.AR,
				ISR:                p.ISR,
			})
		}
		return &protocol.TopicMetadata{
			TopicErrorCode:    protocol.ErrNone.Code(),
			Topic:             topic.Topic,
			PartitionMetadata: partitionMetadata,
		}
	}
	if len(req.Topics) == 0 {
		// Respond with metadata for all topics
		// how to handle err here?
		_, topics, _ := state.GetTopics()
		topicMetadata = make([]*protocol.TopicMetadata, 0, len(topics))
		for _, topic := range topics {
			topicMetadata = append(topicMetadata, topicMetadataFn(topic, protocol.ErrNone))
		}
	} else {
		topicMetadata = make([]*protocol.TopicMetadata, 0, len(req.Topics))
		for _, topicName := range req.Topics {
			_, topic, err := state.GetTopic(topicName)
			if topic == nil {
				topicMetadata = append(topicMetadata, topicMetadataFn(&structs.Topic{Topic: topicName}, protocol.ErrUnknownTopicOrPartition))
			} else if err != nil {
				topicMetadata = append(topicMetadata, topicMetadataFn(&structs.Topic{Topic: topicName}, protocol.ErrUnknown.WithErr(err)))
			} else {
				topicMetadata = append(topicMetadata, topicMetadataFn(topic, protocol.ErrNone))
			}
		}
	}
	resp := &protocol.MetadataResponse{
		Brokers:       brokers,
		TopicMetadata: topicMetadata,
	}
	return resp
}

func (b *Broker) handleFetch(header *protocol.RequestHeader, r *protocol.FetchRequest) *protocol.FetchResponses {
	fresp := &protocol.FetchResponses{
		Responses: make([]*protocol.FetchResponse, len(r.Topics)),
	}
	received := time.Now()
	for i, topic := range r.Topics {
		fr := &protocol.FetchResponse{
			Topic:              topic.Topic,
			PartitionResponses: make([]*protocol.FetchPartitionResponse, len(topic.Partitions)),
		}
		for j, p := range topic.Partitions {
			replica, err := b.replicaLookup.Replica(topic.Topic, p.Partition)
			if err != nil {
				fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
					Partition: p.Partition,
					ErrorCode: protocol.ErrReplicaNotAvailable.Code(),
				}
				continue
			}
			if replica.Partition.Leader != b.config.ID {
				fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
					Partition: p.Partition,
					ErrorCode: protocol.ErrNotLeaderForPartition.Code(),
				}
				continue
			}
			rdr, rdrErr := replica.Log.NewReader(p.FetchOffset, p.MaxBytes)
			if rdrErr != nil {
				fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
					Partition: p.Partition,
					ErrorCode: protocol.ErrUnknown.Code(),
				}
				continue
			}
			b := new(bytes.Buffer)
			var n int32
			for n < r.MinBytes {
				if r.MaxWaitTime != 0 && int32(time.Since(received).Nanoseconds()/1e6) > r.MaxWaitTime {
					break
				}
				// TODO: copy these bytes to outer bytes
				nn, err := io.Copy(b, rdr)
				if err != nil && err != io.EOF {
					fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
						Partition: p.Partition,
						ErrorCode: protocol.ErrUnknown.Code(),
					}
					break
				}
				n += int32(nn)
				if err == io.EOF {
					break
				}
			}

			fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
				Partition:     p.Partition,
				ErrorCode:     protocol.ErrNone.Code(),
				HighWatermark: replica.Log.NewestOffset(),
				RecordSet:     b.Bytes(),
			}
		}

		fresp.Responses[i] = fr
	}
	return fresp
}

// isController returns true if this is the cluster controller.
func (b *Broker) isController() bool {
	return b.isLeader()
}

func (b *Broker) isLeader() bool {
	return b.raft.State() == raft.Leader
}

// createPartition is used to add a partition across the cluster.
func (b *Broker) createPartition(partition structs.Partition) error {
	_, err := b.raftApply(structs.RegisterPartitionRequestType, structs.RegisterPartitionRequest{
		partition,
	})
	return err
}

// startReplica is used to start a replica on this, including creating its commit log.
func (b *Broker) startReplica(replica *Replica) protocol.Error {
	b.Lock()
	defer b.Unlock()
	b.logger.Debug("start replica", log.String("replica/partition", dump(replica.Partition)))
	state := b.fsm.State()
	_, topic, err := state.GetTopic(replica.Partition.Topic)
	if err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	if topic == nil {
		return protocol.ErrUnknownTopicOrPartition
	}
	var isReplica bool
	for _, replica := range topic.Partitions[replica.Partition.ID] {
		if replica == b.config.ID {
			isReplica = true
			break
		}
	}
	if !isReplica {
		return protocol.ErrReplicaNotAvailable
	}
	if replica.Log == nil {
		log, err := commitlog.New(commitlog.Options{
			Path:            filepath.Join(b.config.DataDir, "data", fmt.Sprintf("%d", replica.Partition.ID)),
			MaxSegmentBytes: 1024,
			MaxLogBytes:     -1,
		})
		if err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
		replica.Log = log
		// TODO: register leader-change listener on r.replica.Partition.id
	}
	return protocol.ErrNone
}

// createTopic is used to create the topic across the cluster.
func (b *Broker) createTopic(topic string, partitions int32, replicationFactor int16) protocol.Error {
	state := b.fsm.State()
	_, t, _ := state.GetTopic(topic)
	if t != nil {
		return protocol.ErrTopicAlreadyExists
	}
	ps := b.buildPartitions(topic, partitions, replicationFactor)
	tt := structs.Topic{
		Topic:      topic,
		Partitions: make(map[int32][]int32),
	}
	for _, partition := range ps {
		tt.Partitions[partition.ID] = partition.AR
	}
	if _, err := b.raftApply(structs.RegisterTopicRequestType, structs.RegisterTopicRequest{Topic: tt}); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	for _, partition := range ps {
		// TODO: think i want to just send this to raft and then create downstream
		if err := b.createPartition(partition); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	// could move this up maybe and do the iteration once
	req := &protocol.LeaderAndISRRequest{
		ControllerID: b.config.ID,
		// TODO ControllerEpoch
	}
	for _, partition := range ps {
		req.PartitionStates = append(req.PartitionStates, &protocol.PartitionState{
			Topic:     partition.Topic,
			Partition: partition.ID,
			// TODO: ControllerEpoch, LeaderEpoch, ZKVersion
			Leader:   partition.Leader,
			ISR:      partition.ISR,
			Replicas: partition.AR,
		})
	}
	// TODO: can optimize this
	for _, s := range b.brokerLookup.Brokers() {
		if s.ID == b.config.ID {
			errCode := b.handleLeaderAndISR(nil, req).ErrorCode
			if protocol.ErrNone.Code() != errCode {
				panic(fmt.Sprintf("failed handling leader and isr: %d", errCode))
			}
		} else {
			c := NewClient(s)
			_, err := c.LeaderAndISR(fmt.Sprintf("%d", b.config.ID), req)
			if err != nil {
				// handle err and responses
				panic(err)
			}
		}

	}
	return protocol.ErrNone
}

func (b *Broker) buildPartitions(topic string, partitionsCount int32, replicationFactor int16) []structs.Partition {
	mems := b.brokerLookup.Brokers()
	memCount := int32(len(mems))
	var partitions []structs.Partition

	for i := int32(0); i < partitionsCount; i++ {
		leader := mems[i%memCount].ID
		replicas := []int32{leader}
		for replica := rand.Int31n(memCount); len(replicas) < int(replicationFactor); replica++ {
			if replica != leader {
				replicas = append(replicas, replica)
			}
			if replica+1 == memCount {
				replica = -1
			}
		}
		partition := structs.Partition{
			Topic:     topic,
			ID:        i,
			Partition: i,
			Leader:    leader,
			AR:        replicas,
			ISR:       replicas,
		}
		partitions = append(partitions, partition)
	}

	return partitions
}

// Leave is used to prepare for a graceful shutdown.
func (s *Broker) Leave() error {
	s.logger.Info("broker: starting leave")

	numPeers, err := s.numPeers()
	if err != nil {
		s.logger.Error("jocko: failed to check raft peers", log.Error("error", err))
		return err
	}

	isLeader := s.isLeader()
	if isLeader && numPeers > 1 {
		future := s.raft.RemoveServer(raft.ServerID(s.config.RaftAddr), 0, 0)
		if err := future.Error(); err != nil {
			s.logger.Error("failed to remove ourself as raft peer", log.Error("error", err))
		}
	}

	if s.serf != nil {
		if err := s.serf.Leave(); err != nil {
			s.logger.Error("failed to leave LAN serf cluster", log.Error("error", err))
		}
	}

	if !isLeader {
		left := false
		limit := time.Now().Add(5 * time.Second)
		for !left && time.Now().Before(limit) {
			// Sleep a while before we check.
			time.Sleep(50 * time.Millisecond)

			// Get the latest configuration.
			future := s.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				s.logger.Error("failed to get raft configuration", log.Error("error", err))
				break
			}

			// See if we are no longer included.
			left = true
			for _, server := range future.Configuration().Servers {
				if server.Address == raft.ServerAddress(s.config.RaftAddr) {
					left = false
					break
				}
			}
		}
	}

	return nil
}

// Shutdown is used to shutdown the broker, its serf, its raft, and so on.
func (b *Broker) Shutdown() error {
	b.logger.Info("shutting down broker")
	b.shutdownLock.Lock()
	defer b.shutdownLock.Unlock()

	if b.shutdown {
		return nil
	}
	b.shutdown = true
	defer close(b.shutdownCh)

	if b.serf != nil {
		b.serf.Shutdown()
	}

	if b.raft != nil {
		b.raftTransport.Close()
		future := b.raft.Shutdown()
		if err := future.Error(); err != nil {
			b.logger.Error("failed to shutdown", log.Error("error", err))
		}
		if b.raftStore != nil {
			b.raftStore.Close()
		}
	}

	return nil
}

// Replication.

func (b *Broker) becomeFollower(replica *Replica, cmd *protocol.PartitionState) protocol.Error {
	// stop replicator to current leader
	b.Lock()
	defer b.Unlock()
	if replica.Replicator != nil {
		if err := replica.Replicator.Close(); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	hw := replica.Log.NewestOffset()
	if err := replica.Log.Truncate(hw); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	conn := b.brokerLookup.BrokerByID(raft.ServerID(cmd.Leader))
	r := NewReplicator(ReplicatorConfig{}, replica, NewClient(conn), b.logger)
	replica.Replicator = r
	if !b.config.DevMode {
		r.Replicate()
	}
	return protocol.ErrNone
}

func (b *Broker) becomeLeader(replica *Replica, cmd *protocol.PartitionState) protocol.Error {
	b.Lock()
	defer b.Unlock()
	if replica.Replicator != nil {
		if err := replica.Replicator.Close(); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
		replica.Replicator = nil
	}
	replica.Partition.Leader = cmd.Leader
	replica.Partition.AR = cmd.Replicas
	replica.Partition.ISR = cmd.ISR
	replica.Partition.LeaderEpoch = cmd.ZKVersion
	return protocol.ErrNone
}

func contains(rs []int32, r int32) bool {
	for _, ri := range rs {
		if ri == r {
			return true
		}
	}
	return false
}

// ensurePath is used to make sure a path exists
func ensurePath(path string, dir bool) error {
	if !dir {
		path = filepath.Dir(path)
	}
	return os.MkdirAll(path, 0755)
}

// Atomically sets a readiness state flag when leadership is obtained, to indicate that server is past its barrier write
func (s *Broker) setConsistentReadReady() {
	atomic.StoreInt32(&s.readyForConsistentReads, 1)
}

// Atomically reset readiness state flag on leadership revoke
func (s *Broker) resetConsistentReadReady() {
	atomic.StoreInt32(&s.readyForConsistentReads, 0)
}

// Returns true if this server is ready to serve consistent reads
func (s *Broker) isReadyForConsistentReads() bool {
	return atomic.LoadInt32(&s.readyForConsistentReads) == 1
}

func (s *Broker) numPeers() (int, error) {
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0, err
	}
	raftConfig := future.Configuration()
	var numPeers int
	for _, server := range raftConfig.Servers {
		if server.Suffrage == raft.Voter {
			numPeers++
		}
	}
	return numPeers, nil
}

func (s *Broker) LANMembers() []serf.Member {
	return s.serf.Members()
}

// Replica
type Replica struct {
	BrokerID   int32
	Partition  structs.Partition
	IsLocal    bool
	Log        CommitLog
	Hw         int64
	Leo        int64
	Replicator *Replicator
}
