package main

import (
	"encoding"
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
)

const UNSUPPORTED_VERSION = 35

const API_KEY_FETCH = 1
const API_KEY_APIVERSIONS = 18
const API_KEY_DESCRIBETOPICPARTIONS = 75

var SUPPORTED_APIS map[int16]ApiKeys

func init() {
	SUPPORTED_APIS = make(map[int16]ApiKeys)
	SUPPORTED_APIS[API_KEY_APIVERSIONS] = ApiKeys{
		api_key:     API_KEY_APIVERSIONS,
		min_version: 0,
		max_version: 4,
	}
	SUPPORTED_APIS[API_KEY_DESCRIBETOPICPARTIONS] = ApiKeys{
		api_key:     API_KEY_DESCRIBETOPICPARTIONS,
		min_version: 0,
		max_version: 1,
	}
	SUPPORTED_APIS[API_KEY_FETCH] = ApiKeys{
		api_key:     API_KEY_FETCH,
		min_version: 0,
		max_version: 16,
	}
}

type TaggedBuffer struct {
	tags []string
}

func (t TaggedBuffer) MarshalBinary() ([]byte, error) {
	if len(t.tags) == 0 {
		return []byte{0}, nil
	}
	return nil, fmt.Errorf("Non-Empty TaggedBuffer not implemented")
}

func (t TaggedBuffer) AppendBinary(in []byte) ([]byte, error) {
	if len(t.tags) == 0 {
		in = append(in, 0)
		return in, nil
	}
	return nil, fmt.Errorf("Non-Empty TaggedBuffer not implemented")
}

type RequestOrResponse interface {
	encoding.BinaryAppender
	encoding.BinaryUnmarshaler
}

type Message struct {
	message_size int32
	header       RequestOrResponse
	body         RequestOrResponse
}

func (m Message) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 4)
	data, err = m.header.AppendBinary(data)
	if err != nil {
		return nil, err
	}
	data, err = m.body.AppendBinary(data)
	if err != nil {
		return nil, err
	}
	m.message_size = int32(len(data) - 4)
	binary.BigEndian.PutUint32(data, uint32(m.message_size))
	return data, nil
}

func (m *Message) UnmarshalBinary(in []byte) error {
	m.message_size = int32(binary.BigEndian.Uint32(in))
	return nil
}

type ResponseHeaderV0 struct {
	correlation_id int32
}

func (r ResponseHeaderV0) AppendBinary(in []byte) ([]byte, error) {
	in = binary.BigEndian.AppendUint32(in, uint32(r.correlation_id))
	return in, nil
}

func (r *ResponseHeaderV0) UnmarshalBinary(in []byte) error {
	r.correlation_id = int32(binary.BigEndian.Uint32(in))
	return nil
}

type ResponseHeaderV1 struct {
	correlation_id int32
	tagged_fields  TaggedBuffer
}

func (r ResponseHeaderV1) AppendBinary(in []byte) ([]byte, error) {
	in = binary.BigEndian.AppendUint32(in, uint32(r.correlation_id))
	in, err := r.tagged_fields.AppendBinary(in)
	if err != nil {
		return nil, err
	}
	return in, nil
}

func (r *ResponseHeaderV1) UnmarshalBinary(in []byte) error {
	r.correlation_id = int32(binary.BigEndian.Uint32(in))
	return nil
}

type ResponseBody struct {
	body []byte
}

func (r ResponseBody) AppendBinary(in []byte) ([]byte, error) {
	in = append(in, r.body...)
	return in, nil
}

func (r *ResponseBody) UnmarshalBinary(in []byte) error {
	r.body = in
	return nil
}

type Request struct {
	request_api_key     int16
	request_api_version int16
	correlation_id      int32
	client_id           string
}

func (r *Request) UnmarshalBinary(in []byte) error {
	r.request_api_key = int16(binary.BigEndian.Uint16(in))
	r.request_api_version = int16(binary.BigEndian.Uint16(in[2:]))
	r.correlation_id = int32(binary.BigEndian.Uint32(in[4:]))

	return nil
}

type ApiVersionsV4ResponseBody struct {
	error_code       int16
	api_keys         []ApiKeys
	throttle_time_ms int32
	tag_buffer       TaggedBuffer
}

func (rb ApiVersionsV4ResponseBody) AppendBinary(in []byte) ([]byte, error) {
	in = binary.BigEndian.AppendUint16(in, uint16(rb.error_code))
	var err error
	in = binary.AppendUvarint(in, uint64(1+len(rb.api_keys)))
	for i := range rb.api_keys {
		in, err = rb.api_keys[i].AppendBinary(in)
		if err != nil {
			return nil, err
		}
	}
	in = binary.BigEndian.AppendUint32(in, uint32(rb.throttle_time_ms))
	in, err = rb.tag_buffer.AppendBinary(in)
	if err != nil {
		return nil, err
	}
	return in, nil
}

func (rb *ApiVersionsV4ResponseBody) UnmarshalBinary(in []byte) error {
	return fmt.Errorf("Unimplemented")
}

type ApiKeys struct {
	api_key     int16
	min_version int16
	max_version int16
	tag_buffer  TaggedBuffer
}

func (k ApiKeys) AppendBinary(in []byte) ([]byte, error) {
	in = binary.BigEndian.AppendUint16(in, uint16(k.api_key))
	in = binary.BigEndian.AppendUint16(in, uint16(k.min_version))
	in = binary.BigEndian.AppendUint16(in, uint16(k.max_version))
	in, err := k.tag_buffer.AppendBinary(in)
	if err != nil {
		return nil, err
	}
	return in, nil
}

type FetchResponseV16Body struct {
	throttle_time_ms int32
	error_code       int16
	session_id       int32
	responses        []TopicResponses
}

func (fr *FetchResponseV16Body) UnmarshalBinary(in []byte) error {
	return fmt.Errorf("Unimplemented")
}

func (fr FetchResponseV16Body) AppendBinary(in []byte) ([]byte, error) {
	in = binary.BigEndian.AppendUint32(in, uint32(fr.throttle_time_ms))
	in = binary.BigEndian.AppendUint16(in, uint16(fr.error_code))
	in = binary.BigEndian.AppendUint32(in, uint32(fr.session_id))
	var err error

	in = binary.AppendUvarint(in, uint64(1+len(fr.responses)))
	for _, v := range fr.responses {
		in, err = v.AppendBinary(in)
		if err != nil {
			return nil, err
		}
	}
	return in, nil
}

type TopicResponses struct {
	topic_id      uuid.UUID
	partitions    []Partition
	tagged_fields TaggedBuffer
}

func (tr TopicResponses) AppendBinary(in []byte) ([]byte, error) {
	uuidBytes, err := tr.topic_id.MarshalBinary()
	if err != nil {
		return nil, err
	}
	in = append(in, uuidBytes...)

	in = binary.AppendUvarint(in, uint64(1+len(tr.partitions)))
	for _, v := range tr.partitions {
		in, err = v.AppendBinary(in)
		if err != nil {
			return nil, err
		}
	}
	in, err = tr.tagged_fields.AppendBinary(in)
	if err != nil {
		return nil, err
	}
	return in, nil
}

type Partition struct {
	partition_index        int32
	error_code             int16
	high_watermark         int64
	last_stable_offset     int64
	log_start_offset       int64
	aborted_transactions   []Transaction
	preferred_read_replica int32
	records                []Record
	tagged_fields          TaggedBuffer
}

func (p Partition) AppendBinary(in []byte) ([]byte, error) {
	var err error
	in = binary.BigEndian.AppendUint32(in, uint32(p.partition_index))
	in = binary.BigEndian.AppendUint64(in, uint64(p.log_start_offset))
	in = binary.BigEndian.AppendUint64(in, uint64(p.last_stable_offset))
	in = binary.BigEndian.AppendUint64(in, uint64(p.high_watermark))
	in = binary.BigEndian.AppendUint16(in, uint16(p.error_code))
	for _, v := range p.aborted_transactions {
		in, err = v.AppendBinary(in)
		if err != nil {
			return nil, err
		}
	}
	in = binary.BigEndian.AppendUint32(in, uint32(p.preferred_read_replica))
	for _, v := range p.records {
		in, err = v.AppendBinary(in)
		if err != nil {
			return nil, err
		}
	}

	return in, nil
}

type Transaction struct {
	producer_id     int64
	first_offset_id int64
	tagged_fields   TaggedBuffer
}

func (t Transaction) AppendBinary(in []byte) ([]byte, error) {
	in = binary.BigEndian.AppendUint64(in, uint64(t.producer_id))
	in = binary.BigEndian.AppendUint64(in, uint64(t.first_offset_id))
	in, err := t.tagged_fields.AppendBinary(in)
	if err != nil {
		return nil, err
	}
	return in, nil
}

type Record struct {
	opaque []byte
}

func (r Record) AppendBinary(in []byte) ([]byte, error) {
	return append(in, r.opaque...), nil
}
