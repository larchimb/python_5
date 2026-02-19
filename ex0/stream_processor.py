from typing import Any
from abc import ABC, abstractmethod

class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return(f"Output: {result}")


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        print("\nInitializing Numeric Processor...")


    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid numeric data")
            total = sum(data)
            avg = total/len(data)
            return (f"Processed {len(data)} numeric values, "
                    f"sum={total}, avg={avg:.1f}")
        except ValueError as e:
            print(e)
            return ""

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            for element in data:
                if not type(element) == int:
                    return False
        else:
            return False
        return True

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        print("\nInitializing Text Processor...")

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid alpha data")
            char = len(data)
            words = len(data.split())
            return (f"Processed text: {char} characteres, {words} words")
        except ValueError as e:
            print(e)
            return ""

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        return True

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        print("\nInitializing Log Processor...")

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Data must be [EXAMPLE: Some informations]")
            words = data.split(': ')
            if words[0] == "ERROR":
                return (
                    f"[ALERT] {words[0]} level detected: {words[1]}"
                )
            else:
                return (
                    f"[INFO] {words[0]} level detected: {words[1]}"
                )
        except ValueError as e:
            print(e)
            return ""

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        check = len(data.split(': '))
        if not check == 2:
            return False
        return True

    def format_output(self, result: str) -> str:
        return super().format_output(result)


if __name__ == "__main__":
    data = [1, 2, 3, 4, 5]
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    processor = NumericProcessor()
    print(f"Processing data: {data}")
    if processor.validate(data):
        print("Validation: Numeric data verified")
    print(processor.format_output(processor.process(data)))

    data = "Hello Nexus World"
    processor1 = TextProcessor()
    print(f"Processing data: {data}")
    if processor1.validate(data):
        print("Validation: Text data verified")
    print(processor1.format_output(processor1.process(data)))

    data = "ERROR: Connection timeout"
    processor2 = LogProcessor()
    print(f"Processing data: {data}")
    if processor2.validate(data):
        print("Validation: Log entry verified")
    print(processor2.format_output(processor2.process(data)))

    print("\n=== Polymorphic Processing Demo ===\n"
          "Processing multiple data types through same interface...\n"
          f"Result 1: {processor.process([1, 2, 3])}\n"
          f"Result 2: {processor1.process('Hello Words!')}\n"
          f"Result 3: {processor2.process('INFO: System ready')}\n"
          "\nFoundation systems online. Nexus ready for advanced streams.")
