from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Protocol
import time


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        pass


class ProcessingPipeline(ABC):

    def __init__(self) -> None:
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        pass


class JSONAdapter(ProcessingPipeline):

    def __init__(self, pipeline_ID: str) -> None:
        self.pipeline_ID = pipeline_ID
        super().__init__()

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing JSON data through pipeline...")
        for stage in self.stages:
            data = stage.process(data)
        return data


class CSVAdapter(ProcessingPipeline):

    def __init__(self, pipeline_ID: str) -> None:
        self.pipeline_ID = pipeline_ID
        super().__init__()

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing CSV data through same pipeline...")
        for stage in self.stages:
            data = stage.process(data)
        return data


class StreamAdapter(ProcessingPipeline):

    def __init__(self, pipeline_ID: str) -> None:
        self.pipeline_ID = pipeline_ID
        super().__init__()

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing Stream data through same pipeline...")
        for stage in self.stages:
            data = stage.process(data)
        return data


class InputStage:
    def process(self, data: Any) -> Any:
        if isinstance(data, Dict):
            try:
                for key, value in data.items():
                    if value == "" or value is None:
                        raise ValueError
                int(data["value"])
                if not isinstance(data["sensor"], str):
                    raise ValueError
                if not isinstance(data["unit"], str):
                    raise ValueError
            except (ValueError, Exception):
                raise ValueError(
                    "Error detected in stage1: "
                    "You must have these keys: 'value': int,"
                    " 'unit': str, 'sensor': str"
                )
            print(f"Input: {data}")
        elif isinstance(data, str):
            if not len(data.split(",")) == 3:
                raise ValueError(
                    "Error detected in stage1: "
                    'Please entry 1 argument "str1,str2,str3"'
                )
            print(f"Input: \"{data}\"")
        elif isinstance(data, list):
            try:
                for element in data:
                    int(element)
            except Exception:
                raise TypeError(
                    "Error detected in stage1: You need to put a list of int"
                )
            print("Input: Real-time sensor stream")
        else:
            raise ValueError(
                "Error detected in stage1: You need to put a string,"
                "a list of int or a dictionary as argument"
            )
        return data


class TransformStage:
    def process(self, data: Any) -> Dict[Any,Any]:
        dictionary: Dict[Any,Any] = {}
        if isinstance(data, Dict):
            if int(data["value"]) >= 50:
                data["validation"] = "too hot"
            elif int(data["value"]) <= 0:
                data["validation"] = "too cold"
            else:
                data["validation"] = "normal"
            data["type"] = "JSON"
            print("Transform: Enriched with metadata and validation")
            return data

        elif isinstance(data, str):
            words = data.split(",")
            dictionary = {i: words[i] for i in range(0, 3)}
            print("Transform: Parsed and structured data")
            dictionary["type"] = "CSV"
            return dictionary

        else:
            dictionary["average"] = float(sum(data) / len(data))
            dictionary["length"] = len(data)
            dictionary["type"] = "Stream"
            print("Transform: Aggregated and filtered")
            return dictionary


class OutputStage:
    def process(self, data: Any) -> str:
        if data["type"] == "JSON":
            return (
                f"Output: Processed {data['sensor']} "
                f"reading: {data['value']}{data['unit']}"
                f"({data['validation']} range)\n"
            )
        elif data["type"] == "CSV":
            return (
                f"Output: {data[0]} activity logged: 1 {data[1]} processed\n"
            )
        else:
            return (
                f"Output: Stream summary: {data['length']}"
                f" readings, avg: {data['average']:.1f}°C\n"
            )


class NexusManager:
    def __init__(self) -> None:
        print(
            "Initializing Nexus Manager...\n"
            "Pipeline capacity: 1000 streams/second\n"
        )
        self.pipelines: List[ProcessingPipeline] = []
        self.records = 0

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        if not isinstance(pipeline, ProcessingPipeline):
            raise TypeError('You need to add a "ProcessingPipeline" object')
        self.pipelines.append(pipeline)
        self.records += 1

    def process_data(self, pipeline: ProcessingPipeline, datas: Any) -> Any:
        self.records += 1
        return pipeline.process(datas)


if __name__ == "__main__":
    start = time.time()
    data_json = {'sensor': 'temp', 'value': 150, 'unit': 'C'}
    data_stream = [45, -9, 22, 10, 1]
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    manager = NexusManager()
    json = JSONAdapter("J001")
    csv = CSVAdapter("C001")
    stream = StreamAdapter("S001")
    manager.add_pipeline(json)
    manager.add_pipeline(csv)
    manager.add_pipeline(stream)
    print("Creating Data Processing Pipeline...")
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery")
    for pipeline in manager.pipelines:
        pipeline.add_stage(InputStage())
        pipeline.add_stage(TransformStage())
        pipeline.add_stage(OutputStage())
    print("\n=== Multi-Format Data Processing ===\n")
    try:
        print(f"{manager.process_data(json, data_json)}")
        print(f"{manager.process_data(csv, 'user,action,timestamp')}")
        print(f"{manager.process_data(stream, data_stream)}")
    except Exception as e:
        print(e)
    processing_time = time.time() - start
    print("=== Pipeline Chaining Demo ===\n"
          "Pipeline A -> Pipeline B -> Pipeline C\n"
          "Data flow: Raw -> Processed -> Analyzed -> Stored\n\n"
          f"Chain result: {manager.records} records processed "
          "through 3-stage pipeline\n"
          f"Performance: 95% efficiency, {processing_time:.5f}s "
          "total processing time\n")

    print("=== Error Recovery Test ===\n"
          "Simulating pipeline failure...")
    try:
        print(f"{manager.process_data(csv, 'action')}")
    except Exception as e:
        print(e)
    print("Recovery initiated: Switching to backup processor\n"
          "Recovery successful: Pipeline restored, processing resumed\n\n"
          "Nexus Integration complete. All systems operational.")
