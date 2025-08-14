package ldb

import "github.com/blockloop/scan/v2"

func SetColumnMapper(f func(string) string) {
	scan.ColumnsMapper = f
}

func SetScannerMapper(f func(string) string) {
	scan.ScannerMapper = f
}
