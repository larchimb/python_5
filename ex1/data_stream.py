from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod

class DataStream(ABC):

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    @abstractmethod
    def filter_data(
        self, data_batch: List[Any],criteria: Optional[str] = None
        )-> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        self.dictio = {}
        return self.dictio

    def validate(self, data: Any) -> bool:
        try:
            if isinstance(data, list):
                for element in data:
                    check = len(element.split(':'))
                    if not check == 2:
                        return False
                    words = element.split(':')
                    if not isinstance(words[0], str):
                        return False
                    float(words[1])
            else:
                return False
        except ValueError:
            return False
        return True


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        print("\nInitializing Sensor Stream...")
        self.stream_id = stream_id
        self.type = "Environmental Data"
        print(f"Stream ID: {self.stream_id}, Type: {self.type}")

    def process_batch(self, data_batch: List[Any]) -> str:
        if not self.validate(data_batch):
            return "Datas must be parsed: [str:int, str:int,...]"
        self.data_batch = data_batch
        return (f"{data_batch}")

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
        )-> List[Any]:
        if criteria is None:
            return data_batch
        for element in data_batch:
            words = element.split(':')
            if words[0] == "temp":
                self.temp = int(words[1])
            if words[0] == criteria:
                data_batch.remove(element)
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        super().get_stats()
        try:
            for element in self.data_batch:
                parts = element.split(':')
                self.dictio[parts[0]] = float(parts[1])
        except AttributeError as e:
            print(e)
        return self.dictio


class EventStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        print("\nInitializing Event Stream...")
        self.stream_id = stream_id
        self.type = "System Events"
        print(f"Stream ID: {self.stream_id}, Type: {self.type}")

    def process_batch(self, data_batch: List[Any]) -> str:
        if not self.validate(data_batch):
            return "Datas must be parsed: [str, str,...]"
        self.data_batch = data_batch
        return (f"{data_batch}")

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
        )-> List[Any]:
        if criteria is None:
            criteria = 'error'
        for element in data_batch:
            if element == criteria:
                pass
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        super().get_stats()
        try:
            for i in self.data_batch:
                self.dictio[i] = self.dictio.get(i, 0) + 1
        except AttributeError as e:
            print(e)
        return self.dictio

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            for element in data:
                if not isinstance(element, str):
                    return False
        else:
            return False
        return True





if __name__ == "__main__":
    data = ['temp:22.5', 'humidity:65', 'pressure:1013']
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    stream = SensorStream("SENSOR_001")
    print(f"Processing sensor batch: {stream.process_batch(data)}")
    dictio = stream.get_stats()
    print(f"Sensor analysis: {len(dictio)}"
          f" readings processed, avg temp: {dictio['temp']}")

    data = ['temp:22.5', 'humidity:65', 'pressure:1013']
    stream = EventStream("EVENT_001")