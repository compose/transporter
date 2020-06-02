package data

// Data is an alias for a map so we can add functions unique to transporter.
type Data map[string]interface{}

// Get returns the value associated with the provided key.
func (d Data) Get(key string) interface{} {
	return d[key]
}

// Set will sets the value based on the key.
func (d Data) Set(key string, value interface{}) {
	d[key] = value
}

// Has returns to two value from of the key lookup on a map.
func (d Data) Has(key string) (interface{}, bool) {
	val, ok := d[key]
	return val, ok
}

// Delete removes the data from the map based on the provided key.
func (d Data) Delete(key string) {
	delete(d, key)
}

// AsMap converts the underlying Data d to a map[string]interface{}.
func (d Data) AsMap() map[string]interface{} {
	m := make(map[string]interface{})
	for key := range d {
		m[key] = d[key]
	}
	return m
}
