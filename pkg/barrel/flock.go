// package barrel

// import (
// 	"fmt"
// 	"os"

// 	"github.com/gofrs/flock"
// )

// // Exists returns true if the given path exists on the filesystem.
// func Exists(path string) bool {
// 	if _, err := os.Stat(path); err != nil {
// 		return false
// 	}
// 	return true
// }

// // CreateFlockFile creates a file lock for the database directory.
// func CreateFlockFile(flockFile string) *flock.Flock {
// 	fileLock := flock.New(flockFile)
// 	locked, err := fileLock.TryLock()
// 	if err != nil {
// 		fmt.Printf("Failed to acquire lock")
// 	} else if locked {
// 		fmt.Printf("Lock acquired")
// 	}
// 	return fileLock
// }

// // DestroyFlockFile removes a file lock for the database directory.
// func DestroyFlockFile(flockF *flock.Flock) error {
// 	// Close any open fd.
// 	if err := flockF.Close(); err != nil {
// 		return fmt.Errorf("cannot close fd on file %q: %w", flockF.Path(), err)
// 	}
// 	// Remove the lock file from the filesystem.
// 	if err := os.Remove(flockF.Path()); err != nil {
// 		return fmt.Errorf("cannot remove file %q: %w", flockF.Path(), err)
// 	}
// 	return nil
// }

package barrel

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// createFlockFile creates a file lock for the database directory.
func createFlockFile(flockFile string) (*os.File, error) {
	flockF, err := os.Create(flockFile)
	if err != nil {
		return nil, fmt.Errorf("cannot create lock file %q: %w", flockFile, err)
	}
	if err := unix.Flock(int(flockF.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("cannot acquire lock on file %q: %w", flockFile, err)
	}
	return flockF, nil
}

// destroyFlockFile removes a file lock for the database directory.
func destroyFlockFile(flockF *os.File) error {
	// Unlock the file.
	if err := unix.Flock(int(flockF.Fd()), unix.LOCK_UN); err != nil {
		return fmt.Errorf("cannot unlock lock on file %q: %w", flockF.Name(), err)
	}
	// Close any open fd.
	if err := flockF.Close(); err != nil {
		return fmt.Errorf("cannot close fd on file %q: %w", flockF.Name(), err)
	}
	// Remove the lock file from the filesystem.
	if err := os.Remove(flockF.Name()); err != nil {
		return fmt.Errorf("cannot remove file %q: %w", flockF.Name(), err)
	}
	return nil
}
