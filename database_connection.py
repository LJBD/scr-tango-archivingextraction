import MySQLdb
from _mysql_exceptions import ProgrammingError, OperationalError
import re
from collections import Counter
from matplotlib import pyplot, dates


class DatabaseExtractor(object):
    def __init__(self, host="localhost", username='root', userpass='solaris', database='tdb'):
        self.connection = MySQLdb.connect(host, username, userpass, database)
        self.cursor = self.connection.cursor()
        self.get_all_attributes()

    def get_all_attributes(self):
        self.cursor.execute('SELECT full_name from adt;')
        self.devices_list = [i[0] for i in self.cursor.fetchall()]
        return self.devices_list

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
        try:
            self.cursor.execute(query)
            query_output = self.cursor.fetchall()
        except ProgrammingError as e:
            print 'Can\'t get guery output for %s' % table_name
            print e
            query_output = [([0*i] for i in columns)]
            print query_output
        return_data = []
        for i in xrange(len(columns)):
            data_for_column = [t[i] for t in query_output]
            return_data.append(data_for_column)
        return return_data

    def calculate_time_diffs(self, id):
        times = self.get_attrib_values_between_dates(id, '2015-06-01', '2020-01-01', ['time'])[0]
        print len(times)
        diffs = []
        for i in xrange(1, len(times)):
            diffs.append((times[i] - times[i-1]).total_seconds())
        histogram_data = Counter(diffs)
        return histogram_data

    def plot_selected_attrib_data_in_range(self, re_filter, start, end):
        attributes = self.get_selected_attributes(re_filter)
        dev_dict = self.get_attrib_ids(attributes)
        fig = pyplot.figure()
        ax = fig.add_subplot(111)
        for key in dev_dict.keys():
            columns = ['time', 'value']
            # :TODO: Catch error with wrong column names (caused by read/read-write attributes)
            attr_values = self.get_attrib_values_between_dates(dev_dict[key], start, end, columns)
            ax.plot(attr_values[0], attr_values[1], label=key)
        pyplot.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
        ax.xaxis.set_major_formatter(dates.DateFormatter('%d.%m %H:%M:%S'))
        pyplot.xticks(rotation='vertical')
        pyplot.subplots_adjust(bottom=.3)
        pyplot.show()

    def plot_time_diffs_histogram(self, re_filter):
        attributes = self.get_selected_attributes(re_filter)
        dev_dict = self.get_attrib_ids(attributes)
        for key in dev_dict.keys():
            histogram_data = self.calculate_time_diffs(dev_dict[key])
            pyplot.plot(histogram_data.keys(), histogram_data.values(), '.-', label=key,)
        #pyplot.hist(diffs, bins=5)
        pyplot.yscale('log')
        pyplot.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
        pyplot.show()


# :TODO: Prepare a GUI for this!
def main():
    dbEx = DatabaseExtractor()
    start = '2015-11-18 09:00:00'
    end = '2015-11-18 16:00:00'
    # re_filter = re.compile('R1-C134/MAG/R1.*-PS.*/Current')
    # re_filter = re.compile('PLC/R1-02-03-VAC/REAL/F_R1_02.*TCO.*_R')
    re_filter = re.compile('R1-SGD/VAC/.*IPCU.*/Pressure')
    # re_filter = re.compile('R.*/RF/.*PAPCW.*/Rf.*')
    print dbEx.get_selected_attributes(re_filter)
    dbEx.plot_selected_attrib_data_in_range(re_filter, start, end)
    dbEx.plot_time_diffs_histogram(re_filter)

    # thermocouples = dbEx.get_selected_attributes(re_filter)
    # dev_dict_thermos = dbEx.get_attrib_ids(thermocouples)
    # re_filter = re.compile('PLC/R1-02-03-VAC/REAL/F_R1_02.*TCO.*_R')
    # SHOWING HISTOGRAM OF TIME DIFFS
    # for key in dev_dict_thermos.keys():
    #     print key, dev_dict_thermos[key],
    #     # start = '2015-11-20 10:00:00'
    #     # end = '2015-11-20 10:01:00'
    #     # columns = ['time', 'read_value']
    #     # dbEx.get_attrib_values_between_dates(dev_dict_thermos[key], start, end, columns)
    #     dbEx.calculate_time_diffs(dev_dict_thermos[key])

    # SHOWING SOME DATA FROM THERMOCOUPLES
    # for key in dev_dict_thermos.keys():
    #     start = '2015-11-20 16:00:00'
    #     end = '2015-11-20 20:00:00'
    #     columns = ['time', 'read_value']
    #     attr_values = dbEx.get_attrib_values_between_dates(dev_dict_thermos[key], start, end, columns)
    #     pyplot.plot(attr_values[0], attr_values[1], label=key.split('/')[3])
    # pyplot.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    # pyplot.show()

if __name__ == '__main__':
    main()
