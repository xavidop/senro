package funcs

// ResetForTest clears the registry. It exists only for tests: Register's own
// doc explains why a real program never wants to un-register a function, but
// the registry is process-global state, and this repository's own test gate
// runs `go test -count=2`, which invokes every Test function twice in the
// SAME process. A second call to Register with a name the first iteration
// already used is not a bug in the code under test, it is that iteration
// finding the first one's registration still there, so funcs_test.go calls
// this through t.Cleanup to leave the registry as empty as a second process
// invocation would find it.
//
// Exported so the external funcs_test package can reach it, but this file's
// own "_test.go" suffix keeps it out of every non-test build: it is not part
// of this package's production API.
func ResetForTest() {
	mu.Lock()
	defer mu.Unlock()
	registry = map[string]Func{}
}
