// Command worker imports log directly and knows nothing about config, so a
// change to config does not run it.
package main

import "example.com/mono/log"

func main() { println(log.Line("worker")) }
