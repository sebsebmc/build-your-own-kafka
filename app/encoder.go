package main

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

type Encoder struct {
}

func (e Encoder) Encode(value interface{}) ([]byte, error) {
	out, err := e.encodeInner(value)
	if err != nil {
		return nil, err
	}
	// The first bits of a message are the message length, less the message length
	binary.BigEndian.AppendUint32(out, uint32(len(out)-4))
	return out, nil
}

func (e Encoder) encodeInner(value interface{}) ([]byte, error) {
	out := make([]byte, 0)
	vt := reflect.TypeOf(value)

	fields := reflect.VisibleFields(vt)
	rv := reflect.ValueOf(value)
	for _, field := range fields {
		if !field.IsExported() {
			continue
		}
		val := rv.FieldByIndex(field.Index)
		if val.Kind() == reflect.Interface {
			val = val.Elem().Elem()
		}
		// fmt.Printf("encoding %s type %s\n", field.Name, field.Type.Name())
		switch val.Kind() {
		case reflect.Int16:
			// fmt.Println(val.Int())
			out = binary.BigEndian.AppendUint16(out, uint16(val.Int()))
		case reflect.Int32:
			// fmt.Println(val.Int())
			out = binary.BigEndian.AppendUint32(out, uint32(val.Int()))
		case reflect.Int64:
			// fmt.Println(val.Int())
			out = binary.BigEndian.AppendUint64(out, uint64(val.Int()))
		case reflect.Array, reflect.Slice:
			// Special case []byte
			if val.Type().Elem().Kind() == reflect.Uint8 {
				out = append(out, val.Bytes()...)
				continue
			}
			out = binary.AppendUvarint(out, uint64(1+val.Len()))

			// TODO: We may have an array of primtives or structs... We can only recurse on structs
			if val.Type().Elem().Kind() != reflect.Struct {
				// fmt.Printf("Skipping array of non-struct types: %s\n", val.Type().String())
				continue
			}
			for i := 0; i < val.Len(); i++ {
				out2, err := e.Encode(val.Index(i).Interface())
				if err != nil {
					return nil, err
				}
				out = append(out, out2...)
			}
			// recurse
		case reflect.Struct:
			out2, err := e.Encode(val.Interface())
			if err != nil {
				return nil, err
			}
			out = append(out, out2...)
		default:
			return nil, fmt.Errorf("unable to encode struct %s field %s of type %s", vt.Name(), field.Name, field.Type.Name())
		}
	}
	return out, nil
}
