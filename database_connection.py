import MySQLdb
import re
from collections import Counter
from matplotlib import pyplot


class DatabaseExtractor(object):
    def __init__(self):
        self.connection = MySQLdb.connect('localhost', 'root', 'solaris', 'tdb')
        self.cursor = self.connection.cursor()

    def get_all_attributes(self):
        self.cursor.execute('SELECT full_name from adt;')
        self.devices_list = [i[0] for i in self.cursor.fetchall()]
        #print self.devices_list

    def get_selected_attributes(self, reg_exp_filter):
        matching_devices = filter(reg_exp_filter.match, self.devices_list)
        return matching_devices

    def get_attrib_ids(self, devices_list):
        dev_ids_dict = {}
        for dev in devices_list:
            self.cursor.execute('SELECT id from adt where full_name = "%s";' % dev)
            dev_ids_dict[dev] = self.cursor.fetchall()[0][0]
        return dev_ids_dict

    def get_attrib_values_between_dates(self, id, start_date, end_date, columns):
        """
        This method is used to extract values of an attribute specified by ID from the database.

        :param id: ID of an attribute
        :param start_date: a string containing a date in MySQL format ('YYYY-MM-DD HH:MM:SS')
        :param end_date: a string containing a date in MySQL format ('YYYY-MM-DD HH:MM:SS')
        :param columns: list of columns to be read out (from among: time, read_value, write_value)
        """
        table_name = 'att_%s' % str(id).rjust(5, '0')
        columns_str = str(columns).strip('[').strip(']').replace("'", '')
        query = 'SELECT %s from %s where time > "%s" and time < "%s";' %\
                (columns_str, table_name, start_date, end_date)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def calculate_time_diffs(self, id):
        times = [i[0] for i in self.get_attrib_values_between_dates(id, '2015-06-01', '2020-01-01', ['time'])]
        print len(times)
        diffs = []
        for i in xrange(1, len(times)):
            diffs.append((times[i] - times[i-1]).total_seconds())
        histogram_data = Counter(diffs)
        pyplot.plot(histogram_data.keys(), histogram_data.values(), 'rs')
        #pyplot.hist(diffs, bins=5)
        pyplot.yscale('log')
        pyplot.show()



def main():
    dbEx = DatabaseExtractor()
    dbEx.get_all_attributes()
    re_filter = re.compile('PLC/R1-02-03-VAC/REAL/.*TCO.*_R')
    thermocouples = dbEx.get_selected_attributes(re_filter)
    dev_dict_thermos = dbEx.get_attrib_ids(thermocouples)
    for key in dev_dict_thermos.keys():
        print key, dev_dict_thermos[key],
        # start = '2015-11-20 10:00:00'
        # end = '2015-11-20 10:01:00'
        # columns = ['time', 'read_value']
        # dbEx.get_attrib_values_between_dates(dev_dict_thermos[key], start, end, columns)
        dbEx.calculate_time_diffs(dev_dict_thermos[key])

if __name__ == '__main__':
    main()
