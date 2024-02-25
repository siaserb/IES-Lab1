from csv import reader
from datetime import datetime
from domain.parking import Parking
from domain.aggregated_data import AggregatedData
from domain.accelerometer import Accelerometer
from domain.gps import Gps


class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.cache_data = {}

    def read(self) -> AggregatedData:
        try:
            accelerometer_data = next(reader(self.cache_data["accelerometer"]))
            gps_data = next(reader(self.cache_data["gps"]))
            parking_data = next(reader(self.cache_data["parking"]))

            x, y, z = map(int, accelerometer_data)
            longitude, latitude = map(float, gps_data)
            empty_count = int(parking_data[0])
            return AggregatedData(accelerometer=Accelerometer(x=x, y=y, z=z),
                                  gps=Gps(longitude=longitude, latitude=latitude),
                                  parking=Parking(empty_count, Gps(longitude=longitude, latitude=latitude)),
                                  time=datetime.now())

        except StopIteration:
            self.stopReading()
            StopIteration("Error")
        except ValueError:
            ValueError("Error")

    def startReading(self):
        self.cache_data["accelerometer"] = open(self.accelerometer_filename, 'r')
        self.cache_data["gps"] = open(self.gps_filename, 'r')
        self.cache_data["parking"] = open(self.parking_filename, 'r')

    def stopReading(self):
        self.cache_data["accelerometer"].close()
        self.cache_data["gps"].close()
        self.cache_data["parking"].close()
