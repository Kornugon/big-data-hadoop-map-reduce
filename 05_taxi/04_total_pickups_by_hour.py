from mrjob.job import MRJob
from mrjob.step import MRStep


# terminal:
# python 04_total_pickups_by_hour.py test.txt
# python 04_total_pickups_by_hour.py yellow_tripdata_2016-01.csv
# python 04_total_pickups_by_hour.py yellow_tripdata_2016-01.csv > total_pickups_by_hour.txt


class MRTaxi(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude,
         pickup_latitude, RatecodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type,
         fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount) = line.split(',')

        hour = tpep_pickup_datetime.split()[1][:2]

        yield hour, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRTaxi.run()
