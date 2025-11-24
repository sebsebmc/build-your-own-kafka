package main

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/google/uuid"
)

type Encoder struct {
}

func (e Encoder) Encode(value any) ([]byte, error) {
	out, err := e.encodeInner(value)
	if err != nil {
		return nil, err
	}
	// The first bits of a message are the message length, less the message length
	binary.BigEndian.AppendUint32(out, uint32(len(out)-4))
	return out, nil
}

func (e Encoder) encodeInner(value any) ([]byte, error) {
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
			if val.Type() == reflect.TypeFor[uuid.UUID]() {
				meth := val.MethodByName("MarshalBinary")
				results := meth.Call([]reflect.Value{})
				out = append(out, results[0].Bytes()...)
				continue
			} else if val.Type().Elem().Kind() == reflect.Uint8 {
				innerBytes := val.Bytes()
				out = binary.AppendUvarint(out, uint64(len(innerBytes)))
				out = append(out, innerBytes...)
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

func (e Encoder) Decode(in []byte, val any) (int, error) {
	vt := reflect.TypeOf(val)
	if vt.Kind() != reflect.Pointer {
		return 0, fmt.Errorf("decode requires a pointer to decode into")
	}

	value := reflect.ValueOf(val)
	read, err := e.decodeInner(in, value)
	if err != nil {
		return read, err
	}
	fmt.Println()
	return read, nil
}

// decodeInner decodes into a single value and returns the number of bytes consumed
// from in.
func (e Encoder) decodeInner(in []byte, value reflect.Value) (int, error) {
	innerType := value.Type()

	consumed := 0
	switch innerType.Kind() {
	case reflect.Int8:
		fmt.Print(" ", int8(in[consumed]), "\n")
		value.Set(reflect.ValueOf(int8(in[consumed])))
		consumed += 1
	case reflect.Int16:
		fmt.Print(" ", int16(binary.BigEndian.Uint16(in[consumed:consumed+2])), "\n")
		value.Set(reflect.ValueOf(int16(binary.BigEndian.Uint16(in[consumed : consumed+2]))))
		consumed += 2
	case reflect.Int32:
		fmt.Print(" ", int32(binary.BigEndian.Uint32(in[consumed:consumed+4])), "\n")
		value.Set(reflect.ValueOf(int32(binary.BigEndian.Uint32(in[consumed : consumed+4]))))
		consumed += 4
	case reflect.Int64:
		fmt.Print(" ", int64(binary.BigEndian.Uint32(in[consumed:consumed+8])), "\n")
		value.Set(reflect.ValueOf(int64(binary.BigEndian.Uint32(in[consumed : consumed+8]))))
		consumed += 8
	case reflect.Struct:
		// TODO: look for UnmarshalBinary? We don't know how many bytes we read though
		read, err := e.decodeFields(in[consumed:], value)
		if err != nil {
			return consumed + read, err
		}
		consumed += read
	case reflect.Array:
		if innerType == reflect.TypeFor[uuid.UUID]() {
			var uuid uuid.UUID
			err := uuid.UnmarshalBinary(in[consumed : consumed+16])
			if err != nil {
				return consumed, err
			}
			fmt.Print(" ", uuid, "\n")
			value.Set(reflect.ValueOf(uuid))
			consumed += 16
		}
	case reflect.Interface, reflect.Pointer:
		innerVal := value.Elem()
		read, err := e.decodeInner(in[consumed:], innerVal)
		if err != nil {
			return consumed + read, err
		}
		consumed += read
	case reflect.String:
		fmt.Println(" ", string(in))
		value.SetString(string(in))
		return len(in), nil
	}
	return consumed, nil
}

func (e Encoder) decodeFields(in []byte, value reflect.Value) (int, error) {
	fields := reflect.VisibleFields(value.Type())

	consumed := 0
	for _, v := range fields {
		fieldVal := value.FieldByIndex(v.Index)
		fmt.Print(v.Name)
		switch v.Type.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			read, err := e.decodeInner(in[consumed:], fieldVal)
			if err != nil {
				return consumed, err
			}
			consumed += read
		case reflect.String:
			tag := v.Tag.Get("string")
			if tag == "" || tag == "compact" {
				length, read := binary.Uvarint(in[consumed:])
				if read <= 0 {
					return consumed, fmt.Errorf("unable to read compact string length, bad varint")
				}
				consumed += read
				read, err := e.decodeInner(in[consumed:consumed+int(length)], fieldVal)
				if err != nil {
					return consumed, err
				}
				consumed += read
			} else if tag == "nullable" {
				length := int16(binary.BigEndian.Uint16(in[consumed:]))
				consumed += 2
				if length == -1 {
					continue
				}
				read, err := e.decodeInner(in[consumed:consumed+int(length)], fieldVal)
				if err != nil {
					return consumed, err
				}
				consumed += read
			}
		case reflect.Interface:
			innerVal := fieldVal.Elem()
			read, err := e.decodeInner(in[consumed:], innerVal)
			if err != nil {
				return consumed, err
			}
			consumed += read
		case reflect.Slice:
			length, read := binary.Uvarint(in[consumed:])
			fmt.Printf(" %d %d\n", length-1, int64(length-1))
			if read <= 0 {
				return consumed, fmt.Errorf("unable to read compact array length, bad varint")
			}
			consumed += read
			if length == 0 {
				continue
			}
			sliceVal := reflect.MakeSlice(v.Type, int(length-1), int(length-1))
			fieldVal.Set(sliceVal)
			for i := 0; i < int(length-1); i++ {
				read, err := e.decodeInner(in[consumed:], sliceVal.Index(i))
				if err != nil {
					return consumed, err
				}
				consumed += read
			}
		case reflect.Struct:
			fmt.Print(": \n")
			if fieldVal.Type() == reflect.TypeFor[TaggedBuffer]() {
				consumed += 1 // Assuming empty TaggedBuffers for now
				fieldVal.Set(reflect.ValueOf(TaggedBuffer{}))
				continue
			}
			read, err := e.decodeInner(in[consumed:], fieldVal)
			if err != nil {
				return consumed, err
			}
			consumed += read
		case reflect.Array:
			if v.Type == reflect.TypeFor[uuid.UUID]() {
				read, err := e.decodeInner(in[consumed:], fieldVal)
				if err != nil {
					return consumed, err
				}
				consumed += read
			}
		default:
			fmt.Printf("Unable to decode %s %s\n", v.Type.String(), v.Name)
		}

	}
	return consumed, nil
}
