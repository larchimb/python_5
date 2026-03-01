from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Protocol, Collection

class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        pass

class ProcessingPipeline(ABC):
    stages: List[ProcessingStage] = []

    def add_stage(self, stage:ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        for stage in self.stages:
            data = stage.process(data)
        return data

class JSONAdaptater(ProcessingPipeline):

    def __init__(self, pipeline_ID: str) -> None:
        self.pipeline_ID = pipeline_ID

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing JSON data through pipeline...")
        return(super().process(data))


class CSVAdaptater(ProcessingPipeline):

    def __init__(self, pipeline_ID: str) -> None:
        self.pipeline_ID = pipeline_ID

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing CSV data through same pipeline...")
        return(super().process(data))


class StreamAdaptater(ProcessingPipeline):

    def __init__(self, pipeline_ID: str) -> None:
        self.pipeline_ID = pipeline_ID

    def process(self, data: Any) -> Union[str, Any]:
        print("Processing Stream data through same pipeline...")
        return(super().process(data))
    

class InputStage():
    def __init__(self):
        print("Stage 1: Input validation and parsing")
        
    def process(self, data: Any) -> Any:
        if isinstance(data, Dict):
            try:
                for key, value in data.items():
                    if value == "" or value == None:
                        raise ValueError
                int(data.get('value'))
                # data.get('unit')
                # data.get('sensor')
            except Exception:
                raise KeyError("Error detected in stage1: You must have these keys: 'value', 'unit', 'sensor'")
        elif isinstance(data, str):
            if not len(data.split(',')) == 3:
                raise Exception("Error detected in stage1: Please entry 1 argument\"str1,str2,str3\"") 
        elif isinstance(data, list):
            try:
                for element in data:
                    int(element)
            except Exception:
                raise TypeError("Error detected in stage1: You need to put a list of int")
        else:
            raise Exception("Error detected in stage1: You need to put a string, a list of int or a dictionary as argument")
        if isinstance(data, List):
            print("Input: Real-time sensor stream")
        else:
            print(f"Input: {data}")
        return data


class TransformStage():
    def __init__(self):
        print("Stage 2: Data transformation and enrichment")

    def process(self, data: Any) -> Dict:
        dictionary: Dict = {}
        if isinstance(data, Dict):
            if data['value'] >= 50:
                data['validation'] = 'too hot'
            elif data['value'] <= 0:
                data['validation'] = 'too cold'
            else:
                data['validation'] = 'normal'
            data['type'] = "JSON"
            print("Transform: Enriched with metadata and validation")
            return data

        elif isinstance(data, str):
            words = data.split(',')
            dictionary = {i : words[i] for i in range(0, 3)}
            print("Transform: Parsed and structured data")
            dictionary['type'] = "CSV"
            return dictionary
        
        else:
            dictionary['average'] = float(sum(data) / len(data))
            dictionary['lenght'] = len(data)
            dictionary['type'] = "Stream"
            print("Transform: Aggregated and filtered")
            return dictionary

        
class OutputStage():
    def __init__(self):
        print("Stage 3: Output formatting and delivery")

    def process(self, data: Any) -> str:
        if data['type'] == "JSON":
            return(f"Output: Processed {data['sensor']} reading: {data['value']}{data['unit']} ({data['validation']} range)\n")
        elif data['type'] == "CSV":
            return(f"Output: {data[0]} activity logged: 1 {data[1]} processed\n")
        elif data['type'] == "Stream":
            return(f"Output: Stream summary: {data['lenght']} readings, avg: {data['average']:.1f}°C\n")

class NexusManager():
    
    def __init__(self) -> None:
        print("Initializing Nexus Manager...\n"
              "Pipeline capacity: 1000 streams/second\n")
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        if not isinstance(pipeline, ProcessingPipeline):
            raise TypeError("You need to add a \"ProcessingPipeline\" object")
        self.pipelines.append(pipeline)

    def process_data(self, pipeline: ProcessingPipeline, datas: Any) -> None:
        pipeline.process(datas)


if __name__ == "__main__":
    data_stream = [45, -9, 22, 10, 1]
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    manager = NexusManager()
    json = JSONAdaptater("J001")
    csv = CSVAdaptater("C001")
    stream = StreamAdaptater("S001")
    print("Creating Data Processing Pipeline...")
    json.add_stage(InputStage())
    csv.add_stage(TransformStage())
    stream.add_stage(OutputStage())
    print("\n=== Multi-Format Data Processing ===\n")
    try:
        print(f"{json.process({"sensor": "", "value": 150, "unit": "°C"})}")
        print(f"{csv.process("user,action,timestamp")}")
        print(f"{stream.process(data_stream)}")
        manager.add_pipeline(json)
        manager.add_pipeline(csv)
        manager.add_pipeline(stream)
    except Exception as e:
        print({e})
    print(f"{len(manager.pipelines)}")

