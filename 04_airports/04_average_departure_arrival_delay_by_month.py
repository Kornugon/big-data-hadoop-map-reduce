from mrjob.job import MRJob
from mrjob.step import MRStep


# terminal:
# python 04_average_departure_arrival_delay_by_month.py test.txt
# python 04_average_departure_arrival_delay_by_month.py flights.csv


class MRFlights(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (year, month, day, day_of_week, airline, flight_number, tail_number,
         origin_airport, destination_airport, scheduled_departure, departure_time,
         departure_delay, taxi_out, wheels_off, scheduled_time, elapsed_time,
         air_time, distance, wheels_on, taxi_in, scheduled_arrival, arrival_time,
         arrival_delay, diverted, cancelled, cancellation_reason, air_system_delay,
         security_delay, airline_delay, late_aircraft_delay,
         weather_delay) = line.split(',')

        """
        tak jak nizej wybadalismy ze nie mozemy przekonwertowac na float pustego stringa
        
        try:
            departure_delay = float(departure_delay)
        except:
            yield None, departure_delay
        """

        if departure_delay == '':
            departure_delay = 0

        if arrival_delay == '':
            arrival_delay = 0

        departure_delay = float(departure_delay)
        arrival_delay = float(arrival_delay)
        month = int(month)

        yield f'{month:02d}', (departure_delay, arrival_delay)

    def reducer(self, key, values):
        total_deep_delay = 0
        total_arr_delay = 0
        num_elements = 0
        for value in values:
            total_deep_delay += value[0]
            total_arr_delay += value[1]
            num_elements += 1
        yield key, (total_deep_delay/num_elements, total_arr_delay/num_elements)


if __name__ == '__main__':
    MRFlights.run()
