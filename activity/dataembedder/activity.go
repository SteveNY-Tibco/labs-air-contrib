/*
 * Copyright © 2020. TIBCO Software Inc.
 * This file is subject to the license terms contained
 * in the license file that is distributed with this file.
 */

package dataembedder

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync"

	"github.com/SteveNY-Tibco/labs-lightcrane-contrib/common/util"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
)

var log = logger.GetLogger("tibco-f1-DataEmbedder-0")

var initialized bool = false

const (
	sTargets              = "Targets"
	iConsumer             = "Consumer"
	iProducer             = "Producer"
	iTargetData           = "TargetData"
	iInputDataCollection  = "InputDataCollection"
	oOutputDataCollection = "OutputDataCollection"
)

type DataEmbedder struct {
	metadata  *activity.Metadata
	mux       sync.Mutex
	dataTypes map[string]map[string]string
}

func NewActivity(metadata *activity.Metadata) activity.Activity {
	aDataEmbedder := &DataEmbedder{
		metadata:  metadata,
		dataTypes: make(map[string]map[string]string),
	}

	return aDataEmbedder
}

func (a *DataEmbedder) Metadata() *activity.Metadata {

	return a.metadata
}

func (a *DataEmbedder) Eval(context activity.Context) (done bool, err error) {

	log.Info("[Eval] entering ........ ")

	producer, ok := context.GetInput(iProducer).(string)
	if !ok {
		return false, errors.New("Invalid producer ... ")
	}
	log.Info("[Eval]  producer : ", producer)

	consumer, ok := context.GetInput(iConsumer).(string)
	if !ok {
		return false, errors.New("Invalid consumer ... ")
	}
	log.Info("[Eval]  consumer : ", consumer)

	targetData, ok := context.GetInput(iTargetData).(map[string]interface{})
	if !ok {
		return false, errors.New("Invalid targetData ... ")
	}
	log.Info("[Eval]  targetData : ", targetData)

	inputDataCollection, ok := context.GetInput(iInputDataCollection).([]interface{})
	if !ok {
		return false, errors.New("Invalid inputDataCollection ... ")
	}
	log.Info("[Eval]  inputDataCollection : ", inputDataCollection)

	outputDataCollection := make([]interface{}, len(inputDataCollection))
	for index, data := range inputDataCollection {
		outputDataCollection[index] = data
	}

	dataTypes, _ := a.getEnrichedDataType(context)
	var newValue interface{}
	for key, value := range targetData {
		log.Info("[Eval]  key : ", key, ", value : ", value)
		dataType := "String"
		if nil != dataTypes && "" != dataTypes[key] {
			dataType = dataTypes[key]
		}

		log.Info("[Eval]  dataType 01 : ", dataType)
		if "String" == dataType {
			var objectValue map[string]interface{}
			err := json.Unmarshal([]byte(value.(string)), &objectValue)
			log.Info("[Eval]  objectValue : ", objectValue)
			if nil != err {
				log.Info("[Eval]  Not object type : ", err.Error())
				newValue = value
			} else {
				newValue = objectValue
				dataType = "Object"
			}
		} else {
			newValue = value
		}

		log.Info("[Eval]  dataType 02 : ", dataType)
		log.Info("[Eval]  newValue : ", newValue)
		log.Info("[Eval]  golang dataType : ", reflect.ValueOf(newValue).Kind().String())
		if nil != value {
			outputDataCollection = append(outputDataCollection, map[string]interface{}{
				"producer": producer,
				"consumer": consumer,
				"name":     key,
				"value":    newValue,
				"type":     dataType,
			})
		}
	}

	log.Info("[Eval]  oOutputDataCollection : ", outputDataCollection)
	context.SetOutput(oOutputDataCollection, outputDataCollection)

	log.Info("[Eval] exit ........ ")

	return true, nil
}

func (a *DataEmbedder) getEnrichedDataType(ctx activity.Context) (map[string]string, error) {
	myId := util.ActivityId(ctx)
	dataTypes := a.dataTypes[myId]

	if nil == dataTypes {
		a.mux.Lock()
		defer a.mux.Unlock()
		if nil == dataTypes {
			dataTypes = make(map[string]string)
			variablesDef, _ := ctx.GetSetting(sTargets)
			log.Info("[getEnrichedDataType] enriched data def = ", variablesDef)
			for _, variableDef := range variablesDef.([]interface{}) {
				variableInfo := variableDef.(map[string]interface{})
				dataTypes[variableInfo["Name"].(string)] = variableInfo["Type"].(string)
			}
			a.dataTypes[myId] = dataTypes
		}
	}
	return dataTypes, nil
}
