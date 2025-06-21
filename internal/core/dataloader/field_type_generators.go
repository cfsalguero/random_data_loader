package dataloader

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// DataGenerator defines the interface for generating random data for a specific column.
type DataGenerator interface {
	GenerateValue() interface{}
}

// StringGenerator generates random string values.
type StringGenerator struct {
	Length int
	Chars  string
}

// NewStringGenerator creates a new string generator with specified length.
func NewStringGenerator(length int) *StringGenerator {
	return &StringGenerator{
		Length: length,
		Chars:  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
	}
}

// GenerateValue generates a random string.
func (g *StringGenerator) GenerateValue() interface{} {
	result := make([]byte, g.Length)
	for i := range result {
		result[i] = g.Chars[rand.IntN(len(g.Chars))]
	}
	return string(result)
}

// IntGenerator generates random integer values.
type IntGenerator struct {
	Min int64
	Max int64
}

// NewIntGenerator creates a new integer generator with specified range.
func NewIntGenerator(min, max int64) *IntGenerator {
	return &IntGenerator{
		Min: min,
		Max: max,
	}
}

// GenerateValue generates a random integer.
func (g *IntGenerator) GenerateValue() interface{} {
	return g.Min + int64(rand.IntN(int(g.Max-g.Min+1)))
}

// FloatGenerator generates random float values.
type FloatGenerator struct {
	Min  float64
	Max  float64
	Prec int // Precision (number of decimal places)
}

// NewFloatGenerator creates a new float generator with specified range and precision.
func NewFloatGenerator(min, max float64, prec int) *FloatGenerator {
	return &FloatGenerator{
		Min:  min,
		Max:  max,
		Prec: prec,
	}
}

// GenerateValue generates a random float.
func (g *FloatGenerator) GenerateValue() interface{} {
	val := g.Min + rand.Float64()*(g.Max-g.Min)
	// Scale for precision
	scale := float64(1)
	for range g.Prec {
		scale *= 10
	}
	return float64(int64(val*scale)) / scale
}

// BoolGenerator generates random boolean values.
type BoolGenerator struct{}

// NewBoolGenerator creates a new boolean generator.
func NewBoolGenerator() *BoolGenerator {
	return &BoolGenerator{}
}

// GenerateValue generates a random boolean.
func (g *BoolGenerator) GenerateValue() interface{} {
	return rand.IntN(2) == 1
}

// DateGenerator generates random date values.
type DateGenerator struct {
	Start time.Time
	End   time.Time
}

// NewDateGenerator creates a new date generator with specified range.
func NewDateGenerator(start, end time.Time) *DateGenerator {
	return &DateGenerator{
		Start: start,
		End:   end,
	}
}

// GenerateValue generates a random date.
func (g *DateGenerator) GenerateValue() interface{} {
	delta := g.End.Unix() - g.Start.Unix()
	sec := int64(rand.IntN(int(delta))) + g.Start.Unix()
	return time.Unix(sec, 0)
}

// TimestampGenerator generates random timestamp values.
type TimestampGenerator struct {
	Start  time.Time
	End    time.Time
	WithTZ bool
}

// NewTimestampGenerator creates a new timestamp generator with specified range.
func NewTimestampGenerator(start, end time.Time, withTZ bool) *TimestampGenerator {
	return &TimestampGenerator{
		Start:  start,
		End:    end,
		WithTZ: withTZ,
	}
}

// GenerateValue generates a random timestamp.
func (g *TimestampGenerator) GenerateValue() interface{} {
	delta := g.End.Unix() - g.Start.Unix()
	sec := int64(rand.IntN(int(delta))) + g.Start.Unix()
	nsec := int64(rand.IntN(1000000000))
	ts := time.Unix(sec, nsec)
	if g.WithTZ {
		return ts
	}
	return ts.UTC().Format(time.DateTime) // Strip timezone for TIMESTAMP WITHOUT TIME ZONE
}

// EnumGenerator generates random values from a predefined set.
type EnumGenerator struct {
	Values []string
}

// NewEnumGenerator creates a new enum generator with specified values.
func NewEnumGenerator(values []string) *EnumGenerator {
	return &EnumGenerator{
		Values: values,
	}
}

// GenerateValue generates a random enum value.
func (g *EnumGenerator) GenerateValue() interface{} {
	return g.Values[rand.IntN(len(g.Values))]
}

// JSONGenerator generates random JSON objects.
type JSONGenerator struct {
	Fields     int    // Number of fields in the object
	Depth      int    // Maximum nesting depth
	ArrayItems int    // Maximum number of items in arrays
	Format     string // "mysql" or "postgres"
}

// NewJSONGenerator creates a new JSON generator.
func NewJSONGenerator(fields, depth, arrayItems int, format string) *JSONGenerator {
	return &JSONGenerator{
		Fields:     fields,
		Depth:      depth,
		ArrayItems: arrayItems,
		Format:     format,
	}
}

// GenerateValue generates a random JSON object.
func (g *JSONGenerator) GenerateValue() interface{} {
	json := g.generateObject(0)
	if g.Format == "postgres" {
		return json
	}
	// MySQL expects a string representation
	return json
}

// generateObject creates a random JSON object with the specified depth.
func (g *JSONGenerator) generateObject(depth int) string {
	if depth >= g.Depth {
		return `"leaf_value_` + randomString(5) + `"`
	}

	fields := rand.IntN(g.Fields) + 1
	parts := make([]string, fields)

	for i := range fields {
		key := "key_" + randomString(3)
		var value string

		switch rand.IntN(4) {
		case 0:
			// String
			value = `"value_` + randomString(5) + `"`
		case 1:
			// Number
			value = strconv.Itoa(rand.IntN(1000))
		case 2:
			// Object (if not too deep)
			if depth < g.Depth-1 {
				value = g.generateObject(depth + 1)
			} else {
				value = `"leaf_value_` + randomString(5) + `"`
			}
		case 3:
			// Array
			items := rand.IntN(g.ArrayItems) + 1
			elements := make([]string, items)
			for j := range items {
				if depth < g.Depth-1 && rand.IntN(2) == 0 {
					elements[j] = g.generateObject(depth + 1)
				} else {
					elements[j] = `"item_` + randomString(3) + `"`
				}
			}
			value = "[" + strings.Join(elements, ", ") + "]"
		}

		parts[i] = `"` + key + `": ` + value
	}

	return "{" + strings.Join(parts, ", ") + "}"
}

// UUIDGenerator generates random UUID values.
type UUIDGenerator struct{}

// NewUUIDGenerator creates a new UUID generator.
func NewUUIDGenerator() *UUIDGenerator {
	return &UUIDGenerator{}
}

// GenerateValue generates a random UUID.
func (g *UUIDGenerator) GenerateValue() interface{} {
	return uuid.NewString()
}

// IPGenerator generates random IP addresses.
type IPGenerator struct {
	IPv6 bool
}

// NewIPGenerator creates a new IP address generator.
func NewIPGenerator(ipv6 bool) *IPGenerator {
	return &IPGenerator{
		IPv6: ipv6,
	}
}

// GenerateValue generates a random IP address.
func (g *IPGenerator) GenerateValue() interface{} {
	if g.IPv6 {
		// Generate IPv6
		parts := make([]string, 8)
		for i := range parts {
			parts[i] = fmt.Sprintf("%x", rand.IntN(65536))
		}
		return strings.Join(parts, ":")
	}

	// Generate IPv4
	return fmt.Sprintf("%d.%d.%d.%d",
		rand.IntN(256), rand.IntN(256),
		rand.IntN(256), rand.IntN(256))
}

// BinaryGenerator generates random binary data.
type BinaryGenerator struct {
	Length int
}

// NewBinaryGenerator creates a new binary data generator.
func NewBinaryGenerator(length int) *BinaryGenerator {
	return &BinaryGenerator{
		Length: length,
	}
}

func (g *BinaryGenerator) GenerateValue() interface{} {
	// GenerateValue generates random binary data.
	data := make([]byte, 0, g.Length)
	for range data {
		data = append(data, byte(rand.IntN(256))) // Random byte
	}

	return data
}

// GeometryGenerator generates random simple geometry objects (for spatial types).
type GeometryGenerator struct {
	Type string // point, linestring, polygon
}

// NewGeometryGenerator creates a new geometry generator.
func NewGeometryGenerator(geomType string) *GeometryGenerator {
	return &GeometryGenerator{
		Type: strings.ToLower(geomType),
	}
}

// GenerateValue generates a random geometry in WKT format.
func (g *GeometryGenerator) GenerateValue() interface{} {
	switch g.Type {
	case "point":
		x := rand.Float64()*360 - 180 // longitude: -180 to 180
		y := rand.Float64()*180 - 90  // latitude: -90 to 90
		return fmt.Sprintf("POINT(%f %f)", x, y)

	case "linestring":
		points := rand.IntN(3) + 2 // At least 2 points
		parts := make([]string, points)
		for i := range points {
			x := rand.Float64()*360 - 180
			y := rand.Float64()*180 - 90
			parts[i] = fmt.Sprintf("%f %f", x, y)
		}
		return fmt.Sprintf("LINESTRING(%s)", strings.Join(parts, ", "))

	case "polygon":
		points := rand.IntN(3) + 4 // At least 4 points for a closed polygon
		parts := make([]string, points)

		// Generate a rough circle-like polygon
		centerX := rand.Float64()*360 - 180
		centerY := rand.Float64()*180 - 90
		radius := rand.Float64() * 10

		for i := range points - 1 {
			angle := 2 * float64(i) * 3.14159 / float64(points-1)
			x := centerX + radius*0.5*float64(rand.IntN(10)+5)*0.1*float64(math.Cos(angle))
			y := centerY + radius*0.5*float64(rand.IntN(10)+5)*0.1*float64(math.Sin(angle))
			parts[i] = fmt.Sprintf("%f %f", x, y)
		}
		// Close the polygon by repeating the first poislognt
		parts[points-1] = parts[0]

		return fmt.Sprintf("POLYGON((%s))", strings.Join(parts, ", "))

	default:
		return "POINT(0 0)" // Default fallback
	}
}

// MoneyGenerator generates random monetary values.
type MoneyGenerator struct {
	Min float64
	Max float64
}

// NewMoneyGenerator creates a new money generator.
func NewMoneyGenerator(min, max float64) *MoneyGenerator {
	return &MoneyGenerator{
		Min: min,
		Max: max,
	}
}

// GenerateValue generates a random monetary value.
func (g *MoneyGenerator) GenerateValue() interface{} {
	value := g.Min + rand.Float64()*(g.Max-g.Min)
	// Format with 2 decimal places
	return fmt.Sprintf("%.2f", value)
}

// IntervalGenerator generates random time intervals.
type IntervalGenerator struct {
	MinHours int
	MaxHours int
}

// NewIntervalGenerator creates a new interval generator.
func NewIntervalGenerator(minHours, maxHours int) *IntervalGenerator {
	return &IntervalGenerator{
		MinHours: minHours,
		MaxHours: maxHours,
	}
}

// GenerateValue generates a random interval.
func (g *IntervalGenerator) GenerateValue() interface{} {
	hours := g.MinHours + rand.IntN(g.MaxHours-g.MinHours+1)
	minutes := rand.IntN(60)
	seconds := rand.IntN(60)

	return fmt.Sprintf("%d hours %d minutes %d seconds", hours, minutes, seconds)
}

// BitStringGenerator generates random bit strings.
type BitStringGenerator struct {
	Length int
}

// NewBitStringGenerator creates a new bit string generator.
func NewBitStringGenerator(length int) *BitStringGenerator {
	return &BitStringGenerator{
		Length: length,
	}
}

// GenerateValue generates a random bit string.
func (g *BitStringGenerator) GenerateValue() interface{} {
	bits := make([]byte, g.Length)
	for i := range bits {
		if rand.IntN(2) == 1 {
			bits[i] = '1'
		} else {
			bits[i] = '0'
		}
	}
	return string(bits)
}

// Helper function to generate random strings.
func randomString(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = chars[rand.IntN(len(chars))]
	}
	return string(result)
}
