// Package closer provides utility to close readers or writers chain in LIFO order.
package closer

type Closers []func() error

func (v *Closers) Append(closers ...func() error) *Closers {
	*v = append(*v, closers...)
	return v
}

func (v *Closers) Close() error {
	if v != nil {
		for i := len(*v) - 1; i >= 0; i-- {
			if err := (*v)[i](); err != nil {
				return err
			}
		}
	}
	return nil
}
