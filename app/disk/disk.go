package disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"

	"github.com/google/uuid"
)

const METADATA_LOG_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
const LOGS_DIR = "/tmp/kraft-combined-logs"

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32 //`lengthFor:"self-12"`
	PartitionLeaderEpoch int32
	VersionMagic         int8
	CRC                  int32
	Attributes           int16
	LastOffsetData       int32
	BaseTimestamp        int64
	MaxTimestemp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordsLength        int32        `lengthFor:"Records" binary:"int32"`
	Records              []DiskRecord `length:"RecordsLength"`
}

type DiskRecord struct {
	Length         int64 `binary:"varint"` // lengthFor:"self"
	Attributes     int8
	TimestampDelta int64       `binary:"varint"`
	OffsetDelta    int64       `binary:"varint"`
	KeyLength      int64       `binary:"varint" lengthFor:"Key"`
	Key            []byte      `length:"KeyLength"`
	ValueLength    int64       `binary:"varint" lengthFor:"Value"` //This is a byte length for the Feature Record
	Value          FramedValue `length:"ValueLength"`              // This needs to be polymorphic
	Headers        []RecordHeader
}

type Record struct {
	Data []byte
}

func (r Record) GetFrameVersion() int8 {
	return -1
}

func (r Record) GetType() int8 {
	return -1
}

func (r Record) GetVersion() int8 {
	return -1
}

type TaggedBuffer struct {
	TaggedFields []byte
}

type FramedValue interface {
	GetFrameVersion() int8
	GetType() int8
	GetVersion() int8
}

type FrameDetails struct {
	FrameVersion int8
	Type         int8
	Version      int8
}

func (fd FrameDetails) GetFrameVersion() int8 {
	return fd.FrameVersion
}

func (fd FrameDetails) GetType() int8 {
	return fd.Type
}

func (fd FrameDetails) GetVersion() int8 {
	return fd.Type
}

type FeatureLevelRecord struct {
	FrameDetails
	Name         string
	FeatureLevel int16
	TaggedFields TaggedBuffer
}

type TopicRecord struct {
	FrameDetails
	Name         string
	TopicId      uuid.UUID
	TaggedFields TaggedBuffer
}

type PartitionRecord struct {
	FrameDetails
	PartitionId      int32
	TopicId          uuid.UUID
	Replicas         []int32
	Isr              []int32
	RemovingReplicas []int32
	AddingReplicas   []int32
	Leader           int32
	LeaderEpoch      int32
	PartitionEpoch   int32
	Directories      []uuid.UUID
	TaggedFields     TaggedBuffer
}

type RecordHeader struct {
}

type DiskManager struct {
	metadata Metadata
}

func (dm *DiskManager) WriteRecord(dt *Topic, partitionIdx int32, record []byte) error {
	pdir := fmt.Sprintf("%s-%d", dt.Name, 0)
	filename := path.Join(LOGS_DIR, pdir, "00000000000000000000.log")
	fh, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	slog.Debug("Writing Records", "file", filename)
	if err != nil {
		return err
	}
	defer fh.Close()
	e := Encoder{}
	bytes, err := e.Encode(RecordBatch{BaseOffset: 0, Records: []DiskRecord{
		{
			Value: Record{
				Data: record,
			},
			//TODO: FramedValue actually needs to become some other interface
			//TODO: Would be good to add some tag support for encoding length
		},
	}})
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint32(bytes[8:12], uint32(len(bytes)-12))
	fh.Write(bytes)
	return nil
}

type Metadata struct {
	topics     map[string]uuid.UUID
	topicNames map[uuid.UUID]string
	partitions map[uuid.UUID]*Topic
}

type Topic struct {
	Name       string
	Id         uuid.UUID
	Partitions []TopicPartition
}

func (t *Topic) HasPartition(partitionIdx int32) bool {
	for _, v := range t.Partitions {
		if v.PartitionId == partitionIdx && t.Id == v.TopicId {
			return true
		}
	}
	return false
}

type TopicPartition struct {
	PartitionId      int32
	TopicId          uuid.UUID
	Replicas         []int32
	Isr              []int32
	RemovingReplicas []int32
	AddingReplicas   []int32
	Leader           int32
	LeaderEpoch      int32
	PartitionEpoch   int32
	Directories      []uuid.UUID
}

func NewDiskManager() *DiskManager {
	dm := new(DiskManager)

	dm.metadata.topics = make(map[string]uuid.UUID)
	dm.metadata.topicNames = make(map[uuid.UUID]string)
	dm.metadata.partitions = make(map[uuid.UUID]*Topic)

	return dm
}

func (dm *DiskManager) LoadMetadata() error {
	fh, err := os.Open(METADATA_LOG_PATH)
	if err != nil {
		return fmt.Errorf("unable to open metadata file: %v", err)
	}
	defer fh.Close()

	mdBytes, err := io.ReadAll(fh)
	if err != nil {
		return fmt.Errorf("unable to read metadata: %v", err)
	}

	e := Encoder{}
	consumed := 0
	for consumed < len(mdBytes) {
		batchOffset := int64(binary.BigEndian.Uint64(mdBytes[consumed:])) // record offset
		slog.Debug("Loading Metadata", "batchNum", batchOffset)
		length := int(binary.BigEndian.Uint32(mdBytes[consumed+8:]))
		rb := RecordBatch{}
		rbLength, err := e.Decode(mdBytes[consumed:consumed+length+12], &rb)
		consumed += rbLength
		if err != nil {
			return err
		}
		slog.Info("Loaded Metadata", "records", len(rb.Records))

		for _, r := range rb.Records {
			switch rec := r.Value.(type) {
			case *FeatureLevelRecord:
			case *TopicRecord:
				slog.Info("Found topic metadata", "name", rec.Name, "id", rec.TopicId)
				dm.metadata.topics[rec.Name] = rec.TopicId
				dm.metadata.topicNames[rec.TopicId] = rec.Name
				dm.metadata.partitions[rec.TopicId] = &Topic{Name: rec.Name, Id: rec.TopicId}
			case *PartitionRecord:
				topic, ok := dm.metadata.partitions[rec.TopicId]
				if !ok {
					slog.Warn("Partition for topic not yet seen")
				}
				slog.Info("Found topic partition metadata", "topic", topic.Name, "id", rec.TopicId, "partition", rec.PartitionId)
				topic.Partitions = append(topic.Partitions, TopicPartition{
					rec.PartitionId,
					rec.TopicId,
					rec.Replicas,
					rec.Isr,
					rec.RemovingReplicas,
					rec.AddingReplicas,
					rec.Leader,
					rec.LeaderEpoch,
					rec.PartitionEpoch,
					rec.Directories,
				})
			}
		}
	}
	return nil
}

// func (dm *DiskManager) LoadTopicPartitions(name string) error {
// 	topicId, ok := dm.metadata.topics[name]
// 	if !ok {
// 		return
// 	}
// 	topic := dm.metadata.partitions[topicId]
// 	for _, p := range topic.Partitions {
// 		p.Directories
// 	}
// }

func (dm DiskManager) GetTopic(name string) (*Topic, error) {
	topicId, ok := dm.metadata.topics[name]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", name)
	}

	// If we want to make this thread-safe we should return a copy
	return dm.metadata.partitions[topicId], nil
}

func (dm DiskManager) GetTopicPartitions(topicId uuid.UUID) (*Topic, error) {
	topic, ok := dm.metadata.partitions[topicId]
	if !ok {
		return nil, fmt.Errorf("no partitions found for topicId %s", topicId)
	}
	// If we want to make this thread-safe we should return a copy
	return topic, nil
}

func (dm DiskManager) LoadRecords(tp TopicPartition, partition int32) ([]byte, error) {
	topicName := dm.metadata.topicNames[tp.TopicId]
	pdir := fmt.Sprintf("%s-%d", topicName, partition)
	fh, err := os.Open(path.Join(LOGS_DIR, pdir, "00000000000000000000.log"))
	if err != nil {
		return nil, err
	}

	logBytes, err := io.ReadAll(fh)
	if err != nil {
		return nil, err
	}

	return logBytes, nil
}
