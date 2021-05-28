package godal

import "sync"

var errorHandlerMu sync.Mutex
var errorHandlerIndex int

// ErrorHandler is a function that can be used to override godal's default behavior
// of treating all messages with severity >= CE_Warning as errors. When an ErrorHandler
// is passed as an option to a godal function, all logs/errors emitted by gdal will be passed
// to this function, which can decide wether the parameters correspond to an actual error
// or not.
//
// If the ErrorHandler returns nil, the parent function will not return an error. It is up
// to the ErrorHandler to log the message if needed.
//
// If the ErrorHandler returns an error, that error will be returned as-is to the caller
// of the parent function
type ErrorHandler func(ec ErrorCategory, code int, msg string) error

type errorHandlerWrapper struct {
	fn     ErrorHandler
	errors []error
}

var errorHandlers = make(map[int]*errorHandlerWrapper)

func registerErrorHandler(fn ErrorHandler) int {
	errorHandlerMu.Lock()
	defer errorHandlerMu.Unlock()
	errorHandlerIndex++
	for errorHandlerIndex == 0 || errorHandlers[errorHandlerIndex] != nil {
		errorHandlerIndex++
	}
	errorHandlers[errorHandlerIndex] = &errorHandlerWrapper{fn: fn}
	return errorHandlerIndex
}

func getErrorHandler(i int) *errorHandlerWrapper {
	errorHandlerMu.Lock()
	defer errorHandlerMu.Unlock()
	return errorHandlers[i]
}

func unregisterErrorHandler(i int) {
	errorHandlerMu.Lock()
	defer errorHandlerMu.Unlock()
	delete(errorHandlers, i)
}

type errorAndLoggingOpts struct {
	eh     ErrorHandler
	config []string
}

type errorCallback struct {
	fn ErrorHandler
}

type errorAndLoggingOption interface {
	setErrorAndLoggingOpt(elo *errorAndLoggingOpts)
}

func ErrLogger(fn ErrorHandler) interface {
	errorAndLoggingOption
	OpenOption
} {
	return errorCallback{fn}
}

func (ec errorCallback) setErrorAndLoggingOpt(elo *errorAndLoggingOpts) {
	elo.eh = ec.fn
}

func (ec errorCallback) setOpenOption(oo *openOptions) {
	oo.errorHandler = ec.fn
}
