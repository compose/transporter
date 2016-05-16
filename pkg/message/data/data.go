package data

type Data map[string]interface{}

func (d Data) Get(key string) interface{} {
	return d[key]
}

func (d Data) Set(key string, value interface{}) {
	d[key] = value
}

func (d Data) Has(key string) (interface{}, bool) {
	val, ok := d[key]
	return val, ok
}

func (d Data) Delete(key string) {
	delete(d, key)
}

func (d Data) AsMap() map[string]interface{} {
	m := make(map[string]interface{})
	for key := range d {
		m[key] = d[key]
	}
	return m
}
