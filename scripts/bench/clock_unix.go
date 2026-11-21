//go:build !windows

package main

import "time"

var clockOrigin = time.Now()

func clockNow() int64                        { return time.Since(clockOrigin).Nanoseconds() }
func clockElapsed(start int64) time.Duration { return time.Duration(clockNow() - start) }
