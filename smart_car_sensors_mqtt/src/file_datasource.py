import sys
from csv import reader
from datetime import datetime
from domain.aggregated_data import AggregatedData
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking


class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accelerometer_csv_reader = None
        self.gps_csv_reader = None
        self.parking_cvs_reader = None

    def read(self) -> AggregatedData:
        accelerometer_row = next(self.accelerometer_csv_reader, None)
        if not accelerometer_row:
            sys.exit("file end")
        x, y, z = accelerometer_row

        gps_row = next(self.gps_csv_reader, None)
        if not gps_row:
            sys.exit("file end")
        longitude, latitude = gps_row

        parking_row = next(self.parking_cvs_reader, None)
        if not parking_row:
            sys.exit("file end")
        empty_count, parking_longitude, parking_latitude = parking_row

        return AggregatedData(Accelerometer(x, y, z),
                              Gps(longitude, latitude),
                              Parking(empty_count, Gps(parking_longitude, parking_latitude)),
                              datetime.now())

    def startReading(self, *args, **kwargs):
        self.accelerometer_csv_reader = reader(
            open(self.accelerometer_filename, 'r'))
        self.gps_csv_reader = reader(
            open(self.gps_filename, 'r'))
        self.parking_cvs_reader = reader(
            open(self.parking_filename, 'r'))
        # Skip the header row if it exists
        next(self.accelerometer_csv_reader, None)
        next(self.gps_csv_reader, None)
        next(self.parking_cvs_reader, None)

    def stopReading(self, *args, **kwargs):
        if self.accelerometer_csv_reader:
            self.accelerometer_csv_reader.close()
        if self.gps_csv_reader:
            self.gps_csv_reader.close()
        if self.parking_cvs_reader:
            self.parking_cvs_reader.close()
