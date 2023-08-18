package main

import (
	"C"
	"github.com/iyacontrol/fluent-bit-clickhouse/out"
	"sync"

	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

var rw sync.Mutex

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "clickhouse", "Clickhouse Output Plugin.!")
}

// FLBPluginInit ctx (context) pointer to fluentbit context (state/ c code)
//
//export FLBPluginInit
func FLBPluginInit(_ unsafe.Pointer) int {
	return out.Client.Connect()
}

// FLBPluginFlush is called from fluent-bit when data need to be sent. is called from fluent-bit when data need to be sent.
//
//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, _ *C.char) int {
	rw.Lock()
	defer rw.Unlock()
	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))
	return out.Client.Flush(dec)
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
