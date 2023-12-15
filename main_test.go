package main

import (
	"testing"
)

var mockLine = `{"type":"Feature","id":1,"geometry":{"type":"Point","coordinates":[-93.25535583496094,44.98938751220703]},"properties":{"Accuracy":16.58573341369629,"Activity":"Stationary","Elevation":256.4858703613281,"Heading":347.74163818359375,"Name":"Rye13","Pressure":95.55844116210938,"Speed":0,"Time":"2023-12-08T10:04:10.017Z","UUID":"05C63745-BFA3-4DE3-AF2F-CDE2173C0E11","UnixTime":1702029850,"Version":"V.customizableCatTrackHat"}}`
var mockLineLowPrecision = `{"type":"Feature","id":1,"geometry":{"type":"Point","coordinates":[-93.25,44.98]},"properties":{"Accuracy":16.58573341369629,"Activity":"Stationary","Elevation":256.4858703613281,"Heading":347.74163818359375,"Name":"Rye13","Pressure":95.55844116210938,"Speed":0,"Time":"2023-12-08T10:04:10.017Z","UUID":"05C63745-BFA3-4DE3-AF2F-CDE2173C0E11","UnixTime":1702029850,"Version":"V.customizableCatTrackHat"}}`

func TestGetCatTrackCellID(t *testing.T) {
	cellID := getTrackCellID(mockLine)
	t.Log(cellID, cellID.ToToken())

	// cellID = getTrackCellID(mockLineLowPrecision)
	// t.Log(cellID, cellID.ToToken(), cellID.Level(), cellID.IsLeaf())
	/*
		https://docs.s2cell.aliddell.com/en/stable/s2_concepts.html#truncation
			lsb = 1 << (2 * (30 - level))
			truncated_cell_id = (cell_id & -lsb) | lsb
	*/
	// var lsb uint64 = 1 << (2 * (30 - cellLevel))
	// truncatedCellID := (uint64(cellID) & -lsb) | lsb
	truncatedCellIDS2 := cellIDWithLevel(cellID, defaultCellLevel)
	t.Log(truncatedCellIDS2, truncatedCellIDS2.ToToken(), truncatedCellIDS2.Level(), truncatedCellIDS2.IsLeaf())

}
