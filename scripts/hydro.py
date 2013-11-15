from xlsparser import XLSParser
from datetime import datetime
from netCDF4 import Dataset
import calendar

def generate(filepath,worksheet,output_file,data_range,units):
    parser = XLSParser()
    with open(filepath, 'r') as f:
        doc = f.read()
    info = parser.extract_worksheets(doc)
    nccl = info[worksheet]
    #header_line = 3
    #columns = nccl[header_line]
    #data_range = (4, 66)
    data_rows = nccl[data_range[0]:data_range[1]]
    print 'Generating',output_file
    nc = Dataset(output_file, 'w')
    nc.createDimension('time', len(data_rows)*12)
    nc.GDAL = "GDAL 1.9.2, released 2012/10/08"
    nc.history = "Created dynamically in IPython Notebook 2013-11-14"
    nc.title = nccl[0][0]
    nc.summary = nccl[1][0]
    nc.naming_authority = 'GLOS'
    nc.source = 'GLERL'
    nc.standard_name_vocabulary = "http://www.cgd.ucar.edu/cms/eaton/cf-metadata/standard_name.html"
    nc.project = 'GLOS'
    nc.Conventions = "CF-1.6"
    time = nc.createVariable('time', 'i8', ('time',))
    time.standard_name = 'time'
    time.units = 'seconds since 1970-01-01'
    time.long_name = 'Time'
    time.axis = 'T'
    precip = nc.createVariable('precipitation', 'f8', ('time',), fill_value=-99.0)
    precip.standard_name = 'precipitation_amount'
    precip.units = units
    for i,row in enumerate(data_rows):
        for j in xrange(12):
            the_date = datetime(row[0], j+1, 1)
            timestamp = calendar.timegm(the_date.utctimetuple())
            time[i*12 + j] = timestamp
            value = row[j+1]
            if value != u'':
                precip[i*12 + j] = value

    nc.close() 

class NBS:
    filepath = ''
    lake = ''
    @classmethod
    def nbs_comp_mm_lakeprc(cls):
        filepath = cls.filepath
        worksheet = 'NBS_comp_mm _LakePrc'
        outputfile = '../nc/%s/NBS_comp_mm_LakePrc.nc' % cls.lake
        units = 'mm'
        generate(filepath, worksheet, outputfile, (4, 66), units)


    @classmethod
    def nbs_comp_cms_lakeprc(cls):
        filepath = cls.filepath
        worksheet = 'NBS_comp_cms_LakePrc'
        outputfile = '../nc/%s/NBS_comp_cms_LakePrc.nc' % cls.lake
        units = 'm3 s-1'
        generate(filepath, worksheet, outputfile, (4, 66), units)

    @classmethod
    def nbs_comp_mm_landprc(cls):
        filepath = cls.filepath
        worksheet = 'NBS_comp_mm_LandPrc'
        outputfile = '../nc/%s/NBS_comp_mm_LandPrc.nc' % cls.lake
        units = 'mm'
        generate(filepath, worksheet, outputfile, (4, 67), units)

    @classmethod
    def nbs_comp_cms_landprc(cls):
        filepath = cls.filepath
        worksheet = 'NBS_comp_cms_LandPrc'
        outputfile = '../nc/%s/NBS_comp_cms_LandPrc.nc' % cls.lake
        units = 'mm'
        generate(filepath, worksheet, outputfile, (4, 67), units)

    @classmethod
    def prclk(cls):
        filepath = cls.filepath
        worksheet = 'PrcLk'
        outputfile = '../nc/%s/PrcLk.nc' % cls.lake
        units = 'mm'
        generate(filepath, worksheet, outputfile, (4, 116), units)

    @classmethod
    def prcld(cls):
        filepath = cls.filepath
        worksheet = 'PrcLd'
        outputfile = '../nc/%s/PrcLd.nc' % cls.lake
        units = 'mm'
        generate(filepath, worksheet, outputfile, (4, 134), units)

    @classmethod
    def run(cls):
        filepath = cls.filepath
        worksheet = 'Run'
        outputfile = '../nc/%s/Run.nc' % cls.lake
        units = 'mm'
        generate(filepath, worksheet, outputfile, (4, 116), units)

    @classmethod
    def evp(cls):
        filepath = cls.filepath
        worksheet = 'Evp'
        outputfile = '../nc/%s/Evp.nc' % cls.lake
        units = 'mm'
        generate(filepath, worksheet, outputfile, (4, 68), units)

    @classmethod
    def all(cls):
        cls.nbs_comp_mm_lakeprc()
        cls.nbs_comp_cms_lakeprc()
        cls.nbs_comp_mm_landprc()
        cls.nbs_comp_cms_landprc()
        cls.prclk()
        cls.prcld()
        cls.run()
        cls.evp()


class Erie(NBS):
    filepath = '../data/glerl_report/NBS_ERI.xlsx'
    lake = 'erie'

class Huron(NBS):
    filepath = '../data/glerl_report/NBS_HGB.xlsx'
    lake = 'huron'

class MichiganHuron(NBS):
    filepath = '../data/glerl_report/NBS_MHG.xlsx'
    lake = 'michiganhuron'

class Michigan(NBS):
    filepath = '../data/glerl_report/NBS_MIC.xlsx'
    lake = 'michigan'

class Ontario(NBS):
    filepath = '../data/glerl_report/NBS_ONT.xlsx'
    lake = 'ontario'

class StClair(NBS):
    filepath = '../data/glerl_report/NBS_STC.xlsx'
    lake = 'stclair'

class Superior(NBS):
    filepath = '../data/glerl_report/NBS_SUP.xlsx'
    lake = 'superior'


if __name__ == '__main__':
    Erie.all()

    Huron.all()

    MichiganHuron.all()

    Michigan.all()

    Ontario.all()

    StClair.all()

    Superior.all()
