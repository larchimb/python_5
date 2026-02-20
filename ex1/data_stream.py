from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod
import sys

class DataStream(ABC):

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    @abstractmethod
    def initialize(self) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
        )-> List[Any]:
        self.filtered_list = []
        return self.filtered_list

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
                    if (float(words[1]) > sys.maxsize or
                        float(words[1]) < -sys.maxsize):
                        return False
            else:
                return False
        except ValueError:
            return False
        return True


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.type = "Environmental Data"

    def initialize(self) -> str:
        print("\nInitializing Sensor Stream...\n"
              f"Stream ID: {self.stream_id}, Type: {self.type}")
        return f"Processing sensor batch:"


    def process_batch(self, data_batch: List[Any]) -> str:
        if not self.validate(data_batch):
            return "Datas must be parsed: [str:int, str:int, ...]"
        self.data_batch = data_batch
        return (f"{data_batch}")

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
        )-> List[Any]:
        super().filter_data(data_batch)
        if criteria == "high":
            for element in data_batch:
                words = element.split(':')
                if words[0] == "temp" and int(words[1]) > 50:
                    self.filtered_list.append(element)
                if words[0] == "humidity" and int(words[1]) > 80:
                    self.filtered_list.append(element)
                if words[0] == "pressure" and int(words[1]) > 1200:
                    self.filtered_list.append(element)
        elif criteria == "low":
            for element in data_batch:
                words = element.split(':')
                if words[0] == "temp" and int(words[1]) < 0:
                    self.filtered_list.append(element)
                if words[0] == "humidity" and int(words[1]) < 20:
                    self.filtered_list.append(element)
                if words[0] == "pressure" and int(words[1]) < 500:
                    self.filtered_list.append(element)
        else:
            return data_batch
        return self.filtered_list

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        super().get_stats()
        try:
            for element in self.data_batch:
                parts = element.split(':')
                self.dictio[parts[0]] = float(parts[1])
        except AttributeError as e:
            print(e)
        return self.dictio

    def get_resume(self) -> str:
        try:
            return(f"Sensor analysis: {len(self.dictio)}"
          " readings processed, avg temp: "
          f"{self.dictio.get('temp', 'No temp in entry')}")
        except Exception as e:
            return f"{e}"


class TransactionStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.type = "Financial Data"

    def initialize(self) -> str:
        print("\nInitializing Transaction Stream...\n"
              f"Stream ID: {self.stream_id}, Type: {self.type}")
        return f"Processing transactions batch:"

    def process_batch(self, data_batch: List[Any]) -> str:
        if not self.validate(data_batch):
            return "Datas must be parsed: [str:int, str:int, ...]"
        self.data_batch = data_batch
        return (f"{data_batch}")

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
        )-> List[Any]:
        super().filter_data(data_batch)
        try:
            if criteria is None:
                return data_batch
            for element in data_batch:
                words = element.split(':')
                if int(words[1]) > int(criteria):
                    self.filtered_list.append(element)
        except Exception as e:
            print(e)
        return self.filtered_list

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        super().get_stats()
        try:
            for element in self.data_batch:
                parts = element.split(':')
                self.dictio[parts[0]] = self.dictio.get(parts[0], 0) + int(parts[1])
        except Exception as e:
            print(e)
        return self.dictio

    def get_resume(self) -> str:
        try:
            total = self.dictio.get('buy', 0) - self.dictio.get('sell', 0)
            return(f"Transaction analysis: {len(self.dictio)} events,"
                  f" net flow: {str(total)} units")
        except Exception as e:
            return f"{e}"

    def validate(self, data: Any) -> bool:
        try:
            if super().validate(data) is False:
                return False
            for element in data:
                words = element.split(':')
                if (int(words[1]) > sys.maxsize or
                    int(words[1]) < -sys.maxsize):
                    return False
        except Exception:
            return False
        return True


class EventStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.type = "System Events"

    def initialize(self) -> str:
        print("\nInitializing Event Stream...\n"
              f"Stream ID: {self.stream_id}, Type: {self.type}")
        return f"Processing event batch:"

    def process_batch(self, data_batch: List[Any]) -> str:
        if not self.validate(data_batch):
            return "Datas must be parsed: [str, str, ...]"
        self.data_batch = data_batch
        return (f"{data_batch}")

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
        )-> List[Any]:
        super().filter_data(data_batch)
        if criteria is None:
            return data_batch
        for element in data_batch:
            if element == criteria:
                pass
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        super().get_stats()
        try:
            for i in self.data_batch:
                self.dictio[i] = self.dictio.get(i, 0) + 1
        except Exception:
            print("Stats: Batch wasn't intialized in this instance")
        return self.dictio

    def get_resume(self) -> str:
        try:
            return(f"Event analysis: {len(self.data_batch)} events,"
                   f" {self.dictio['error']} error detected")
        except Exception as e:
            return f"{e}"

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            for element in data:
                if not isinstance(element, str):
                    return False
        else:
            return False
        return True


class StreamProcessor():

    def initialize(self, stream: DataStream) -> str:
        return stream.initialize()

    def run(self, stream: DataStream, batch: List[Any]) -> str:
        return stream.process_batch(batch)

    def filter(self, stream: DataStream, batch: List[Any], criteria: str
             ) -> List[Any]:
        return stream.filter_data(batch, criteria)

    def stats(self, stream: DataStream) -> Dict[str, Union[str, int, float]]:
        return stream.get_stats()


if __name__ == "__main__":

    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    processor = StreamProcessor()
    datas = [
        ['temp:22.5', 'humidity:65', 'pressure:1013'],
        ["buy:100", "sell:1500", "buy:75"],
        ["login", "error", "logout", "error"]
        ]
    streams = [
        SensorStream("SENSOR_001"),
        TransactionStream("TRANS_001"),
        EventStream("EVENT_001")
    ]
    i = 0
    for i in range(0, 3):
        print(f"{processor.initialize(streams[i])} {processor.run(streams[i], datas[i])}")
        stats_sensor = processor.stats(streams[i])
        print(streams[i].get_resume())

    print("\n=== Polymorphic Stream Processing ===\n"
          "Processing mixed stream types through unified interface...\n")
