from dataclasses import asdict, dataclass

@dataclass(init=True)
class InstrumentFields:
    name: str = ""

class InstrumentDataset:
    def __init__(self):
        self.fields = InstrumentFields()

    def _toJson(self):
        return asdict(self.fields)

