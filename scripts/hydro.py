from xlsparser import XLSParser
from datetime import datetime
from netCDF4 import Dataset
from jinja2 import Template
from hashlib import sha1
import calendar
import re

class ParserContext:
    filepath      = ''
    worksheet     = ''
    output_file   = ''
    data_range    = tuple()
    units         = ''
    variable      = ''
    standard_name = ''
    fill_value    = -99.0
    category = ''
    def __init__(self,
                 filepath='',
                 worksheet='',
                 output_file='',
                 data_range=None,
                 units='',
                 variable='',
                 standard_name='',
                 fill_value=-99.0,
                 category=''):
        self.filepath      = filepath
        self.worksheet     = worksheet
        self.output_file   = output_file
        self.data_range    = data_range
        self.units         = units
        self.variable      = variable
        self.standard_name = standard_name
        self.fill_value    = fill_value
        self.category = category

class CatalogDataset:
    def __init__(self):
        self.name =''
        self.id = ''
        self.nc_name = ''
        self.keywords = ''
        self.nc_file = ''
        self.title = ''
        self.lat_min = 0.0
        self.lat_max = 0.0
        self.lon_min = 0.0
        self.lon_max = 0.0


category_geo = {
        'erie' : [[41.145270499999995, 43.153226499999995],[-83.56737199999999, -78.645816]],
        'huron' : [[43.030886, 45.925314], [-84.530071, -80.775424]],
        'superior' : [[46.425095,48.723861], [-92.177277,-84.594911]],
        'michigan' : [[41.62,46.03], [-88.27,-84.77]],
        'michiganhuron' : [[41.5586,48.95], [-92.414,-75.659]],
        'ontario': [[43.12,44.33], [-79.80,-75.95]],
        'lakes' : [[41.5586,48.95], [-92.414,-75.659]],
        'stclair' : [[42.3,42.68], [-83.00,-82.43]],
        '':[[41.5586,48.95], [-92.414,-75.659]]
        }


datasets = []

dataset_names = []

def generate_catalog_xml(parser_context):
    global datasets
    parser = XLSParser()
    with open(parser_context.filepath, 'r') as f:
        doc = f.read()
    info = parser.extract_worksheets(doc)
    nccl = info[parser_context.worksheet]

    dataset = CatalogDataset()
    dataset.name = nccl[0][0]
    dataset.nc_name = parser_context.output_file.split('/')[-1]
    dataset.id = sha1(dataset.name + parser_context.output_file).hexdigest()[:8]
    dataset.title = nccl[0][0]
    dataset.keywords = 'GLOS'
    dataset.nc_file = '/var/data-cache/%s' % parser_context.output_file[6:]
    lats, lons = category_geo[parser_context.category]
    dataset.lat_min, dataset.lat_max = lats
    dataset.lon_min, dataset.lon_max = lons
    datasets.append(dataset)

def render(catalog):
    global datasets
    global dataset_names
    with open('catalog_template.xml', 'r') as f:
        catalog_template = f.read()


    template = Template(catalog_template)
    print 'Rendering','../glos_catalog/TDS/glerl/%s.xml'  % catalog 
    with open('../glos_catalog/TDS/glerl/%s.xml' % catalog, 'w') as f:
        f.write(template.render(catalog_name=catalog, datasets=datasets))
    for dataset in datasets:
        dsname = '/'.join([catalog,dataset.id])
        if dsname in dataset_names:
            raise Exception("Duplicate: %s" % dataset.nc_file)
        dataset_names.append(dsname)

    datasets = []


def generate_nc(parser_context):
    parser = XLSParser()
    with open(parser_context.filepath, 'r') as f:
        doc = f.read()
    info = parser.extract_worksheets(doc)
    nccl = info[parser_context.worksheet]
    #header_line = 3
    #columns = nccl[header_line]
    #data_range = (4, 66)
    data_rows = nccl[parser_context.data_range[0]:parser_context.data_range[1]]
    print 'Generating',parser_context.output_file
    nc = Dataset(parser_context.output_file, 'w')
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
    time = nc.createVariable('time', 'f8', ('time',))
    time.standard_name = 'time'
    time.units = 'seconds since 1970-01-01'
    time.long_name = 'Time'
    time.axis = 'T'
    precip = nc.createVariable(parser_context.variable, 'f8', ('time',), fill_value=parser_context.fill_value)
    #precip.standard_name = 'precipitation_amount'
    precip.standard_name = parser_context.standard_name

    precip.units = parser_context.units
    for i,row in enumerate(data_rows):
        for j in xrange(12):
            the_date = datetime(row[0], j+1, 1)
            timestamp = calendar.timegm(the_date.utctimetuple())
            time[i*12 + j] = timestamp
            try:
                value = float(row[j+1])
            except ValueError:
                continue
            except TypeError:
                continue

            precip[i*12 + j] = value
    nc.close() 

def generate(parser_context):
    generate_nc(parser_context)
    #generate_catalog_xml(parser_context)

class NBS:
    filepath      = ''
    lake          = ''
    variable      = 'precipitation'
    standard_name = 'precipitation_amount'
    @classmethod
    def nbs_comp_mm_lakeprc(cls):
        filepath = cls.filepath
        worksheet = 'NBS_comp_mm _LakePrc'
        outputfile = '../nc/%s/NBS_comp_mm_LakePrc.nc' % cls.lake
        units = 'mm'
        parser_context = ParserContext()
        parser_context.filepath = filepath
        parser_context.worksheet = worksheet
        parser_context.output_file = outputfile
        parser_context.data_range = (4,66)
        parser_context.units = units
        parser_context.variable = cls.variable
        parser_context.standard_name = cls.standard_name
        parser_context.category = cls.lake
        generate(parser_context)


    @classmethod
    def nbs_comp_cms_lakeprc(cls):
        filepath = cls.filepath
        worksheet = 'NBS_comp_cms_LakePrc'
        outputfile = '../nc/%s/NBS_comp_cms_LakePrc.nc' % cls.lake
        units = 'm3 s-1'
        parser_context = ParserContext()
        parser_context.filepath = filepath
        parser_context.worksheet = worksheet
        parser_context.output_file = outputfile
        parser_context.data_range = (4,66)
        parser_context.units = units
        parser_context.variable = cls.variable
        parser_context.standard_name = cls.standard_name
        parser_context.category = cls.lake
        generate(parser_context)

    @classmethod
    def nbs_comp_mm_landprc(cls):
        filepath = cls.filepath
        worksheet = 'NBS_comp_mm_LandPrc'
        outputfile = '../nc/%s/NBS_comp_mm_LandPrc.nc' % cls.lake
        units = 'mm'
        parser_context = ParserContext()
        parser_context.filepath = filepath
        parser_context.worksheet = worksheet
        parser_context.output_file = outputfile
        parser_context.data_range = (4,67)
        parser_context.units = units
        parser_context.variable = cls.variable
        parser_context.standard_name = cls.standard_name
        parser_context.category = cls.lake
        generate(parser_context)

    @classmethod
    def nbs_comp_cms_landprc(cls):
        filepath = cls.filepath
        worksheet = 'NBS_comp_cms_LandPrc'
        outputfile = '../nc/%s/NBS_comp_cms_LandPrc.nc' % cls.lake
        units = 'mm'
        parser_context = ParserContext()
        parser_context.filepath = filepath
        parser_context.worksheet = worksheet
        parser_context.output_file = outputfile
        parser_context.data_range = (4,67)
        parser_context.units = units
        parser_context.variable = cls.variable
        parser_context.standard_name = cls.standard_name
        parser_context.category = cls.lake
        generate(parser_context)

    @classmethod
    def prclk(cls):
        filepath = cls.filepath
        worksheet = 'PrcLk'
        outputfile = '../nc/%s/PrcLk.nc' % cls.lake
        units = 'mm'
        parser_context = ParserContext()
        parser_context.filepath = filepath
        parser_context.worksheet = worksheet
        parser_context.output_file = outputfile
        parser_context.data_range = (4,116)
        parser_context.units = units
        parser_context.variable = cls.variable
        parser_context.standard_name = cls.standard_name
        parser_context.category = cls.lake
        generate(parser_context)

    @classmethod
    def prcld(cls):
        filepath = cls.filepath
        worksheet = 'PrcLd'
        outputfile = '../nc/%s/PrcLd.nc' % cls.lake
        units = 'mm'
        parser_context = ParserContext()
        parser_context.filepath = filepath
        parser_context.worksheet = worksheet
        parser_context.output_file = outputfile
        parser_context.data_range = (4,134)
        parser_context.units = units
        parser_context.variable = cls.variable
        parser_context.standard_name = cls.standard_name
        parser_context.category = cls.lake
        generate(parser_context)

    @classmethod
    def run(cls):
        filepath = cls.filepath
        worksheet = 'Run'
        outputfile = '../nc/%s/Run.nc' % cls.lake
        units = 'mm'
        parser_context = ParserContext()
        parser_context.filepath = filepath
        parser_context.worksheet = worksheet
        parser_context.output_file = outputfile
        parser_context.data_range = (4,116)
        parser_context.units = units
        parser_context.variable = cls.variable
        parser_context.standard_name = cls.standard_name
        parser_context.category = cls.lake
        generate(parser_context)

    @classmethod
    def evp(cls):
        filepath = cls.filepath
        worksheet = 'Evp'
        outputfile = '../nc/%s/Evp.nc' % cls.lake
        units = 'mm'
        parser_context = ParserContext()
        parser_context.filepath = filepath
        parser_context.worksheet = worksheet
        parser_context.output_file = outputfile
        parser_context.data_range = (4,68)
        parser_context.units = units
        parser_context.variable = cls.variable
        parser_context.standard_name = cls.standard_name
        parser_context.category = cls.lake
        generate(parser_context)

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

class AirTemperature:
    filepath = '../data/glerl_report/AirTemperature_OverBasin.xlsx'
    datapath = 'airtemp'

    @classmethod
    def generator(cls, worksheet):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = 'deg_C'
        ctxt.variable = 'air_temperature'
        ctxt.standard_name = 'air_temperature'
        ctxt.fill_value = -9999.0
        ctxt.data_range = (4,67)
        cats = {
          'HGB' : 'huron',
          'Sup' : 'superior',
          'Ont' : 'ontario',
          'Grt' : 'lakes',
          'Stc' : 'stclair',
          'Geo' : 'lakes',
          'Eri' : 'erie',
          'Mic' : 'michigan'
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    
    @classmethod
    def all(cls):
        worksheets = [u'HGBAve',
                      u'SupMin',
                      u'StcAve',
                      u'EriMax',
                      u'OntMax',
                      u'StcMin',
                      u'StcMax',
                      u'HGBMin',
                      u'GeoAve',
                      u'SupMax',
                      u'MicMin',
                      u'GrtAve',
                      u'HurMin',
                      u'MHGMax',
                      u'HurMax',
                      u'OntMin',
                      u'OntAve',
                      u'GeoMin',
                      u'MicAve',
                      u'HGBMax',
                      u'EriAve',
                      u'MicMax',
                      u'MHGMin',
                      u'GrtMax',
                      u'SupAve',
                      u'GrtMin',
                      u'MHGAve',
                      u'HurAve',
                      u'EriMin',
                      u'GeoMax']
        for worksheet in worksheets:
            cls.generator(worksheet)

class AirTemperature_OverLand:
    filepath = '../data/glerl_report/AirTemperature_OverLand.xlsx'
    datapath = 'airtemp_ol'

    @classmethod
    def generator(cls, worksheet):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = 'deg_C'
        ctxt.variable = 'air_temperature'
        ctxt.standard_name = 'air_temperature'
        ctxt.fill_value = -9999.0
        ctxt.data_range = (4,67)
        cats = {
          'HGB' : 'huron',
          'Sup' : 'superior',
          'Ont' : 'ontario',
          'Grt' : 'lakes',
          'Stc' : 'stclair',
          'Geo' : 'lakes',
          'Eri' : 'erie',
          'Mic' : 'michigan'
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        worksheets = [
         u'HGBAve',
         u'SupMin',
         u'StcAve',
         u'EriMax',
         u'OntMax',
         u'StcMin',
         u'StcMax',
         u'HGBMin',
         u'GeoAve',
         u'SupMax',
         u'MicMin',
         u'GrtAve',
         u'HurMin',
         u'MHGMax',
         u'HurMax',
         u'OntMin',
         u'OntAve',
         u'GeoMin',
         u'MicAve',
         u'HGBMax',
         u'EriAve',
         u'MicMax',
         u'MHGMin',
         u'GrtMax',
         u'SupAve',
         u'GrtMin',
         u'MHGAve',
         u'HurAve',
         u'EriMin',
         u'GeoMax']
        for worksheet in worksheets:
            cls.generator(worksheet)

class AirTempsOverLake:
    filepath = '../data/glerl_report/AirTemps_OverLake.xls'
    datapath = 'airtemps'

    @classmethod
    def generator(cls, worksheet):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = 'deg_C'
        ctxt.variable = 'air_temperature'
        ctxt.standard_name = 'air_temperature'
        ctxt.fill_value = -9999.0
        ctxt.data_range = (4,67)
        cats = {
          'HGB' : 'huron',
          'Sup' : 'superior',
          'Ont' : 'ontario',
          'Grt' : 'lakes',
          'Stc' : 'stclair',
          'Geo' : 'lakes',
          'Eri' : 'erie',
          'Mic' : 'michigan'
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        worksheets = [
             u'HGBAve',
             u'SupMin',
             u'StcAve',
             u'EriMax',
             u'OntMax',
             u'StcMin',
             u'StcMax',
             u'HGBMin',
             u'GeoAve',
             u'SupMax',
             u'MicMin',
             u'GrtAve',
             u'HurMin',
             u'MHGMax',
             u'HurMax',
             u'OntMin',
             u'OntAve',
             u'GeoMin',
             u'MicAve',
             u'HGBMax',
             u'EriAve',
             u'MicMax',
             u'MHGMin',
             u'GrtMax',
             u'SupAve',
             u'GrtMin',
             u'MHGAve',
             u'HurAve',
             u'EriMin',
             u'GeoMax']
        for worksheet in worksheets:
            cls.generator(worksheet)

class ChangeInStorage:
    filepath = '../data/glerl_report/ChangeInStorage.xls'
    datapath = 'storage'

    @classmethod
    def generator(cls, worksheet, data_range):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = 'm3 s-1'
        ctxt.variable = 'change_in_storage'
        ctxt.standard_name = 'change_in_storage'
        ctxt.fill_value = -9999.0
        ctxt.data_range = data_range
        cats = {
          'HUR' : 'huron',
          'SUP' : 'superior',
          'ONT' : 'ontario',
          'STC' : 'stclair',
          'ERI' : 'erie',
          'MHN' : 'michigan'
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        worksheets = [
                [u'MHN', (5,96)],
                [u'STC', (5,86)],
                [u'MIC', (5,96)],
                [u'HUR', (5,96)],
                [u'SUP', (5, 136)],
                [u'ERI', (5, 96)],
                [u'ONT', (5, 96)]]
        for worksheet, data_range in worksheets:
            cls.generator(worksheet, data_range)


class CloudCoverOverlake:
    filepath = '../data/glerl_report/CloudCover_OverLake.xlsx'
    datapath = 'cloud'

    @classmethod
    def generator(cls, worksheet):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = 'percent'
        ctxt.variable = 'cloud_cover'
        ctxt.standard_name = 'large_scale_cloud_area_fraction'
        ctxt.fill_value = -99.0
        ctxt.data_range = (4,68)
        cats = {
          'HGB' : 'huron',
          'SUP' : 'superior',
          'ONT' : 'ontario',
          'GRT' : 'lakes',
          'STC' : 'stclair',
          'GEO' : 'lakes',
          'ERI' : 'erie',
          'MIC' : 'michigan',
          'MHG' : 'michigan',
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        worksheets = [
             u'GRT',
             u'STC',
             u'MHG',
             u'MIC',
             u'GEO',
             u'HUR',
             u'SUP',
             u'HGB',
             u'ERI',
             u'ONT']
        for worksheet in worksheets:
            cls.generator(worksheet)

class Evaporation:
    filepath = '../data/glerl_report/Evaporation.xlsx'
    datapath = 'evaporation'

    @classmethod
    def generator(cls, worksheet):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = 'mm'
        ctxt.variable = 'evaporation'
        ctxt.standard_name = 'water_evaporation_amount'
        ctxt.fill_value = -99.0
        ctxt.data_range = (4,66)
        cats = {
          'HGB' : 'huron',
          'SUP' : 'superior',
          'ONT' : 'ontario',
          'GRT' : 'lakes',
          'STC' : 'stclair',
          'GEO' : 'lakes',
          'ERI' : 'erie',
          'MIC' : 'michigan',
          'MHG' : 'michigan',
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        worksheets = [
             u'GRT',
             u'STC',
             u'MHG',
             u'MIC',
             u'GEO',
             u'HUR',
             u'SUP',
             u'HGB',
             u'ERI',
             u'ONT']
        for worksheet in worksheets:
            cls.generator(worksheet)

class LevelsBOM:
    filepath = '../data/glerl_report/Levels_BOM.xls'
    datapath = 'levels'

    @classmethod
    def generator(cls, worksheet, data_range):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = 'm'
        ctxt.variable = 'water_level'
        ctxt.standard_name = 'water_level'
        ctxt.fill_value = -99.0
        ctxt.data_range = data_range
        cats = {
          'HGB' : 'huron',
          'SUP' : 'superior',
          'ONT' : 'ontario',
          'GRT' : 'lakes',
          'STC' : 'stclair',
          'GEO' : 'lakes',
          'ERI' : 'erie',
          'MIC' : 'michigan',
          'MHG' : 'michigan',
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        worksheets = [
                 ['MHN', (5,106)],
                 ['STC', (5,96)],
                 ['MIC', (5,96)],
                 ['HUR', (5,96)],
                 ['SUP', (5,146)],
                 ['ERI', (5,106)],
                 ['ONT', (5,106)]]
        for worksheet, data_range in worksheets:
            cls.generator(worksheet,data_range)

class PrecipBasin:
    filepath = '../data/glerl_report/Precip_Basin.xlsx'
    datapath = 'precipbasin'

    @classmethod
    def generator(cls, worksheet, units):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = units
        ctxt.variable = 'precipitation'
        ctxt.standard_name = 'precipitation'
        ctxt.fill_value = -99.0
        ctxt.data_range = (5,116)
        cats = {
          'HGB' : 'huron',
          'SUP' : 'superior',
          'ONT' : 'ontario',
          'GRT' : 'lakes',
          'STC' : 'stclair',
          'GEO' : 'lakes',
          'ERI' : 'erie',
          'MIC' : 'michigan',
          'MHG' : 'michigan',
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        mm_worksheets = [
                 u'MIC_mm',
                 u'GEO_mm',
                 u'ERI_mm',
                 u'ONT_mm',
                 u'MHG_mm',
                 u'SUP_mm',
                 u'HGB_mm',
                 u'HUR_mm',
                 u'GRT_mm',
                 u'STC_mm']
        cm_worksheets = [
                 u'ONT_cms',
                 u'MIC_cms',
                 u'STC_cms',
                 u'SUP_cms',
                 u'GRT_cms',
                 u'HGB_cms',
                 u'HUR_cms',
                 u'ERI_cms',
                 u'GEO_cms',
                 u'MHG_cms']
        for worksheet in mm_worksheets:
            cls.generator(worksheet, 'mm')

        for worksheet in cm_worksheets:
            cls.generator(worksheet, 'm3 s-1')

class PrecipLake:
    filepath = '../data/glerl_report/Precip_Lake.xlsx'
    datapath = 'preciplake'

    @classmethod
    def generator(cls, worksheet, units):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = units
        ctxt.variable = 'precipitation'
        ctxt.standard_name = 'precipitation'
        ctxt.fill_value = -99.0
        ctxt.data_range = (5,116)
        cats = {
          'HGB' : 'huron',
          'SUP' : 'superior',
          'ONT' : 'ontario',
          'GRT' : 'lakes',
          'STC' : 'stclair',
          'GEO' : 'lakes',
          'ERI' : 'erie',
          'MIC' : 'michigan',
          'MHG' : 'michigan',
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        mm_worksheets = [
                 u'MIC_mm',
                 u'GEO_mm',
                 u'ERI_mm',
                 u'ONT_mm',
                 u'MHG_mm',
                 u'SUP_mm',
                 u'HGB_mm',
                 u'HUR_mm',
                 u'GRT_mm',
                 u'STC_mm']
        cm_worksheets = [
                 u'ONT_cms',
                 u'MIC_cms',
                 u'STC_cms',
                 u'SUP_cms',
                 u'GRT_cms',
                 u'HGB_cms',
                 u'HUR_cms',
                 u'ERI_cms',
                 u'GEO_cms',
                 u'MHG_cms']
        for worksheet in mm_worksheets:
            cls.generator(worksheet, 'mm')

        for worksheet in cm_worksheets:
            cls.generator(worksheet, 'm3 s-1')

class PrecipLand:
    filepath = '../data/glerl_report/Precip_Land.xlsx'
    datapath = 'precipland'

    @classmethod
    def generator(cls, worksheet, units):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = units
        ctxt.variable = 'precipitation'
        ctxt.standard_name = 'precipitation'
        ctxt.fill_value = -99.0
        ctxt.data_range = (5,116)
        cats = {
          'HGB' : 'huron',
          'SUP' : 'superior',
          'ONT' : 'ontario',
          'GRT' : 'lakes',
          'STC' : 'stclair',
          'GEO' : 'lakes',
          'ERI' : 'erie',
          'MIC' : 'michigan',
          'MHG' : 'michigan',
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        mm_worksheets = [
                 u'MIC_mm',
                 u'GEO_mm',
                 u'ERI_mm',
                 u'ONT_mm',
                 u'MHG_mm',
                 u'SUP_mm',
                 u'HGB_mm',
                 u'HUR_mm',
                 u'GRT_mm',
                 u'STC_mm']
        cm_worksheets = [
                 u'ONT_cms',
                 u'MIC_cms',
                 u'STC_cms',
                 u'SUP_cms',
                 u'GRT_cms',
                 u'HGB_cms',
                 u'HUR_cms',
                 u'ERI_cms',
                 u'GEO_cms',
                 u'MHG_cms']
        for worksheet in mm_worksheets:
            cls.generator(worksheet, 'mm')

        for worksheet in cm_worksheets:
            cls.generator(worksheet, 'm3 s-1')

class Runoff:
    filepath = '../data/glerl_report/Runoff.xlsx'
    datapath = 'runoff'

    @classmethod
    def generator(cls, worksheet, units):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = units
        ctxt.variable = 'runoff'
        ctxt.standard_name = 'runoff_amount'
        ctxt.fill_value = -99.0
        ctxt.data_range = (5,120)
        cats = {
          'HGB' : 'huron',
          'SUP' : 'superior',
          'ONT' : 'ontario',
          'GRT' : 'lakes',
          'STC' : 'stclair',
          'GEO' : 'lakes',
          'ERI' : 'erie',
          'MIC' : 'michigan',
          'MHG' : 'michigan',
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        mm_worksheets = [
                 u'MIC_mm',
                 u'GEO_mm',
                 u'ERI_mm',
                 u'ONT_mm',
                 u'GRT_mm',
                 u'STC_mm',
                 u'MHG_mm',
                 u'SUP_mm',
                 u'HGB_mm',
                 u'HUR_mm']
        cm_worksheets = [
                 u'ONT_cms',
                 u'ERI_cms',
                 u'GEO_cms',
                 u'MHG_cms',
                 u'SUP_NoO_cms',
                 u'GRT_cms',
                 u'MIC_cms',
                 u'STC_cms',
                 u'SUP_cms',
                 u'HGB_cms',
                 u'HUR_cms']
        pct_worksheets = [
                 u'ERI_Pct',
                 u'HUR_Pct',
                 u'MIC_Pct',
                 u'ONT_Pct',
                 u'SUP_Pct',
                 u'GEO_Pct',
                 u'STC_Pct']
        for worksheet in mm_worksheets:
            cls.generator(worksheet, 'mm')

        for worksheet in cm_worksheets:
            cls.generator(worksheet, 'm3 s-1')

        for worksheet in pct_worksheets:
            cls.generator(worksheet, 'percent')

class WaterTempsModeled:
    filepath = '../data/glerl_report/WaterTemps_Modeled.xlsx'
    datapath = 'watertemps'

    @classmethod
    def generator(cls, worksheet):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = 'deg_C'
        ctxt.variable = 'surface_temperature'
        ctxt.standard_name = 'surface_temperature'
        ctxt.fill_value = -99.0
        ctxt.data_range = (4,66)
        cats = {
          'HGB' : 'huron',
          'SUP' : 'superior',
          'ONT' : 'ontario',
          'GRT' : 'lakes',
          'STC' : 'stclair',
          'GEO' : 'lakes',
          'ERI' : 'erie',
          'MIC' : 'michigan',
          'MHG' : 'michigan',
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        worksheets = [
                 u'STC',
                 u'HGB',
                 u'MIC',
                 u'HUR',
                 u'SUP',
                 u'GEO',
                 u'ERI',
                 u'ONT']
        for worksheet in worksheets:
            cls.generator(worksheet)

class WindSpeedOverlake:
    filepath = '../data/glerl_report/WindSpeed_Overlake.xlsx'
    datapath = 'windspeed'

    @classmethod
    def generator(cls, worksheet):
        ctxt = ParserContext()
        ctxt.worksheet = worksheet
        ctxt.output_file = '../nc/%s/%s.nc' % (cls.datapath, worksheet)
        ctxt.filepath = cls.filepath
        ctxt.units = 'm s-1'
        ctxt.variable = 'wind_speed'
        ctxt.standard_name = 'wind_speed'
        ctxt.fill_value = -99.0
        ctxt.data_range = (4,68)
        cats = {
          'HGB' : 'huron',
          'SUP' : 'superior',
          'ONT' : 'ontario',
          'GRT' : 'lakes',
          'STC' : 'stclair',
          'GEO' : 'lakes',
          'ERI' : 'erie',
          'MIC' : 'michigan',
          'MHG' : 'michigan',
         }
        for k,v in cats.iteritems():
            if k in worksheet:
                ctxt.category = v
                break
        generate(ctxt)

    @classmethod
    def all(cls):
        worksheets = [
                 u'GRT',
                 u'STC',
                 u'MHG',
                 u'MIC',
                 u'GEO',
                 u'HUR',
                 u'SUP',
                 u'HGB',
                 u'ERI',
                 u'ONT']
        for worksheet in worksheets:
            cls.generator(worksheet)

if __name__ == '__main__':
    # NBS Data

    Erie.run()
    Huron.all()
    MichiganHuron.all()
    Michigan.all()
    Ontario.all()
    StClair.all()
    Superior.all()

    render('NBS')

    # Air Temperature

    AirTemperature.all()
    AirTemperature_OverLand.all()
    AirTempsOverLake.all()

    render('AIRTEMPS')


    # Levels BOM
    LevelsBOM.all()

    # Precipitation
    PrecipBasin.all()
    PrecipLake.all()
    PrecipLand.all()
    
    # Change In Storage
    ChangeInStorage.all()

    # Runoff
    Runoff.all()
    
    # Evaporation
    Evaporation.all()


    render('PRECIP')

    # Water Temps Modeled
    WaterTempsModeled.all()
    
    # Wind Speed Over Lake
    WindSpeedOverlake.all()

    # Cloud Cover
    CloudCoverOverlake.all()

    render('ATMO')
