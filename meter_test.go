package dstore

type meterMock struct {
	BytesRead    int
	BytesWritten int
}

func (m *meterMock) AddBytesRead(n int) {
	m.BytesRead += n
}

func (m *meterMock) AddBytesWritten(n int) {
	m.BytesWritten += n
}
