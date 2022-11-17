package modules

// DemoBindingStorage 123
type DemoBindingStorage struct {
	Base
}

// NewDemoBindingStorage 1
func NewDemoBindingStorage() *DemoBindingStorage {
	bs := &DemoBindingStorage{}

	return bs
}

// GetUserFrontendID 1
func (bs *DemoBindingStorage) GetUserFrontendID(uid, frontendType string) (string, error) {
	return "1", nil
}

// PutBinding c
func (bs *DemoBindingStorage) PutBinding(uid string) error {
	return nil
}
