package sshexec

import (
	"fmt"
	"strings"
)

// factPrefix marks a line this package's own scripts wrote, so a login
// shell's banner or motd is not parsed as a fact about the host: `ssh host
// cmd` runs the account's login shell, and bash reads ~/.bashrc even for a
// remote non-interactive shell. A STEP's output cannot be protected the
// same way; the executor's doc says so.
const factPrefix = "senro-fact "

// quote wraps s so a POSIX shell reads it back as exactly one word: single
// quotes suspend every expansion, and the one character they cannot carry
// is closed, escaped and reopened. Every remote path, argument and
// environment entry this package sends goes through here: unquoted, a step
// argument containing a semicolon would be a second command.
func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// quoteAll quotes a whole argv into one shell word list.
func quoteAll(args []string) string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		out = append(out, quote(a))
	}
	return strings.Join(out, " ")
}

// factsScript asks a host what it is: platform, non-interactive search
// path, home directory, and where a credential should be written. The
// runtime directory is chosen ON THE HOST, mirroring secretdir.Root's order
// exactly, because whether this host has a tmpfs is a fact about the host.
const factsScript = `printf '` + factPrefix + `os %s\n' "$(uname -s)"
printf '` + factPrefix + `arch %s\n' "$(uname -m)"
printf '` + factPrefix + `path %s\n' "$PATH"
printf '` + factPrefix + `home %s\n' "$HOME"
if [ -n "${XDG_RUNTIME_DIR:-}" ] && [ -d "${XDG_RUNTIME_DIR:-}" ]; then
	printf '` + factPrefix + `runtime %s\n' "$XDG_RUNTIME_DIR"
elif [ -d /dev/shm ]; then
	printf '` + factPrefix + `runtime %s\n' /dev/shm
else
	printf '` + factPrefix + `runtime %s\n' "${TMPDIR:-/tmp}"
fi`

// parseFacts reads the lines factsScript wrote and ignores everything else.
func parseFacts(out string) map[string]string {
	facts := make(map[string]string, 5)
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSuffix(strings.TrimSpace(line), "\r")
		if !strings.HasPrefix(line, factPrefix) {
			continue
		}
		key, value, ok := strings.Cut(strings.TrimPrefix(line, factPrefix), " ")
		if !ok {
			continue
		}
		facts[key] = value
	}
	return facts
}

// setupScript creates one attempt's directories on the remote host and arms
// the reaper that removes them if this coordinator never gets to.
//
// The reaper is why this is one script: Close does not run when the
// coordinator is killed, and a plaintext credential left on somebody else's
// build host is the failure this design is arranged around. The removal is
// armed on the REMOTE side, before anything is written, by a detached shell
// (nohup with all three descriptors redirected, so sshd closes the session
// rather than waiting on the background job). The rm -rf paths end in a
// random nonce, so they can name nothing a person put there; checkRoot
// refuses a root that would make that untrue.
//
// It also re-reads the platform, so ObservedPlatform has a second,
// independent observation: a round-robin name in front of a mixed fleet
// answers the setup session and the facts probe from different machines.
func setupScript(a attemptPaths, ttlSeconds int) string {
	var b strings.Builder
	b.WriteString("umask 077\n")
	fmt.Fprintf(&b, "SENRO_A=%s\n", quote(a.dir))
	fmt.Fprintf(&b, "SENRO_S=%s\n", quote(a.secretDir))
	b.WriteString("export SENRO_A SENRO_S\n")
	// Armed BEFORE the directories exist rather than after, so a coordinator
	// killed between the mkdir and the arming cannot leave an unreaped
	// directory. Removing a path that never existed is not an error.
	fmt.Fprintf(&b,
		"nohup sh -c 'sleep %d; rm -rf -- \"$SENRO_A\" \"$SENRO_S\"' </dev/null >/dev/null 2>&1 &\n",
		ttlSeconds)
	b.WriteString("mkdir -p \"$SENRO_A\" || exit 1\n")
	for _, dir := range a.mountDirs {
		fmt.Fprintf(&b, "mkdir -p %s || exit 1\n", quote(dir))
	}
	if a.wantSecrets {
		b.WriteString("mkdir -p \"$SENRO_S\" || exit 1\n")
		b.WriteString("chmod 700 \"$SENRO_S\" || exit 1\n")
	}
	b.WriteString(factsScript)
	b.WriteString("\n")
	return b.String()
}

// secretScript writes one credential, reading the value from the session's
// own stdin. Never an argument (visible in `ps`, recorded by auditd, in
// shell history), never an environment variable (/proc/<pid>/environ),
// never a here-document in the command text: stdin bytes on an already
// encrypted channel is the one path with none of those properties.
//
// umask 077 before the redirect rather than a chmod after it: a chmod
// leaves a window in which the file exists and is world readable, and on a
// shared build host that window is the whole vulnerability.
func secretScript(a attemptPaths, file string) string {
	return "umask 077\n" +
		"mkdir -p " + quote(a.secretDir) + " || exit 1\n" +
		"chmod 700 " + quote(a.secretDir) + " || exit 1\n" +
		"cat > " + quote(a.secretDir+"/"+file) + "\n"
}

// runScript is the step's own command, wrapped in exactly enough shell to
// record what it did:
//
//   - The command runs under `env -i`, so it receives the declared
//     environment and nothing else: without it a step would receive the
//     remote account's login environment and, worse, SSH_AUTH_SOCK on a
//     connection with agent forwarding, which hands a build step the
//     operator's own keys. The one addition is PATH, the value EffectiveEnv
//     already reported.
//   - The exit status is written to a file: what separates a command that
//     genuinely exited 255 from ssh's own 255 (see classify).
//   - The wrapper's pid is written to a file, so a cancelled run has
//     something to kill on cleanup rather than only closing a connection.
//
// A wrapper-level failure exits 255 DELIBERATELY, joining ssh's ambiguous
// code: the status file is what resolves that code, and a wrapper that
// failed before the command ran has not written one, making "the command
// never ran" one case rather than a set of sentinel codes a real command
// could collide with.
//
// The `env -i` list is also how a step's trace context crosses, and the
// only way it could: senro sets no SendEnv, so what a step receives is
// decided here rather than by the remote AcceptEnv. It is therefore in the
// command text and visible in `ps`, like every other declared variable;
// that is why a SECRET never travels this way (see secretScript). A
// traceparent names no principal and grants no access.
func runScript(a attemptPaths, dir string, env []string, args []string) string {
	var b strings.Builder
	b.WriteString("umask 022\n")
	fmt.Fprintf(&b, "SENRO_A=%s\n", quote(a.dir))
	if dir != "" {
		fmt.Fprintf(&b, "cd %s || { printf 'senro: cannot enter %%s on this host\\n' %s >&2; exit 255; }\n",
			quote(dir), quote(dir))
	}
	b.WriteString("printf '%s' \"$$\" > \"$SENRO_A/pid\" 2>/dev/null\n")
	b.WriteString("env -i")
	for _, kv := range env {
		b.WriteString(" " + quote(kv))
	}
	// The utility handed to env is a shell that execs the step's real argv
	// out of its positional parameters, not the argv itself: BusyBox env
	// has no `--`, so a program name beginning with a dash would be read as
	// an option to env and one containing "=" as another assignment.
	// `exec "$@"` runs exactly what the pipeline wrote, with no expansion,
	// and the shell is replaced by the program so $? is the program's own.
	b.WriteString(` sh -c 'exec "$@"' senro-step ` + quoteAll(args) + "\n")
	b.WriteString("senro_status=$?\n")
	b.WriteString("printf '%s' \"$senro_status\" > \"$SENRO_A/status\" 2>/dev/null\n")
	b.WriteString("exit \"$senro_status\"\n")
	return b.String()
}

// statusScript reads back what runScript recorded. Its output is prefixed for
// the same reason the facts are: a login banner must not be mistaken for an
// exit code, and here that mistake would report a wrong verdict for a step.
func statusScript(a attemptPaths) string {
	return "if [ -f " + quote(a.dir+"/status") + " ]; then\n" +
		"printf '" + factPrefix + "status %s\\n' \"$(cat " + quote(a.dir+"/status") + ")\"\n" +
		"fi\n"
}

// cleanupScript removes everything this attempt put on the host, and kills
// what it may have left running.
//
// The kill comes first and is best effort: closing an ssh connection is not
// reliably the end of the far-side command, and the wrapper's recorded pid
// is a handle rather than a guarantee (the child is signalled through the
// process group where the host puts them in one). shred is used when the
// host has it and rm otherwise, the same promise secretdir makes locally.
func cleanupScript(a attemptPaths) string {
	var b strings.Builder
	fmt.Fprintf(&b, "SENRO_A=%s\n", quote(a.dir))
	fmt.Fprintf(&b, "SENRO_S=%s\n", quote(a.secretDir))
	b.WriteString("if [ -f \"$SENRO_A/pid\" ]; then\n")
	b.WriteString("senro_pid=$(cat \"$SENRO_A/pid\" 2>/dev/null)\n")
	b.WriteString("if [ -n \"$senro_pid\" ]; then\n")
	b.WriteString("kill -TERM \"-$senro_pid\" 2>/dev/null || kill -TERM \"$senro_pid\" 2>/dev/null\n")
	b.WriteString("fi\n")
	b.WriteString("fi\n")
	b.WriteString("if command -v shred >/dev/null 2>&1 && [ -d \"$SENRO_S\" ]; then\n")
	b.WriteString("find \"$SENRO_S\" -type f -exec shred -u -- {} + 2>/dev/null\n")
	b.WriteString("fi\n")
	b.WriteString("rm -rf -- \"$SENRO_A\" \"$SENRO_S\"\n")
	return b.String()
}

// tarInScript extracts a workspace this coordinator is streaming in. -p
// keeps the executable bit through a non-root extraction; the tar being
// extracted is workspace.WriteTar's, whose modes are 0644 and 0755 with no
// setuid bit to preserve by accident.
func tarInScript(dir string) string {
	return "mkdir -p " + quote(dir) + " || exit 1\n" +
		"cd " + quote(dir) + " || exit 1\n" +
		"exec tar -xpf -\n"
}

// tarOutScript streams a workspace back. `.` rather than a glob, so dotfiles
// and an empty directory both work.
func tarOutScript(dir string) string {
	return "cd " + quote(dir) + " || exit 1\n" +
		"exec tar -cf - .\n"
}
