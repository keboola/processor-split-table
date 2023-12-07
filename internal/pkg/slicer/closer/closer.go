// Package closer provides utility to close readers or writers chain in LIFO order.
package closer

// Closers type is a list of close callbacks, which are invoked in LIFO order.
//
// # Closing Writers Chain
//
// Writer Close method can have the following uses:
//   - releasing resources
//   - flushing internal buffers
//   - write stream tail
//
// For reasons above, it is necessary to call close methods in the correct order, see example bellow:
//
//	WRITE        >>> BufferIn -> GzipWriter -> BufferOut -> os.File
//	OPEN                4     <-     3      <-     2     <-    1            (opening the file is the FIRST step)
//	CLOSE/FLUSH         1     ->     2      ->     3     ->    4            (closing the file is the LAST step)
//
// # Closing Readers Chain
//
// Reader Close method is used only to release resources.
//
// The stream ends with an EndOfFile (io.EOF) error, so there is no risk that we have not read everything.
//
// Therefore, we can close the readers in any order, so we use the same LIFO order.
//
//	READ io.EOF  <<< BufferOut <- GzipReader <- BufferIn <- os.File
//	OPEN                4      <-     3      <-    2     <-    1            (opening the file is the first step)
//	CLOSE              4/1     <->   3/2     <->  2/3    <->  1/4           (closing the file is the FIRST or the LAST step)
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
