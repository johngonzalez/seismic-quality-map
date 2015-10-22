import pandas as pd  # Create Data Frame (as excel tables)
import numpy as np   # Evaluate types from pandas
import re            # Evaluate Regular expressions
import pyodbc        # Save informmation to Microsoft Access Data Base
import pyproj        # Proj coordinates from planes to geographycs (lon, lat)


def promaxdb2df(doc):
    """
    Return pandas DataFrame from csv Promax geometry file.

    Argumets:
    doc -- csv Promax geometry file
    skiprows -- number of skipped rows of header file
    name -- header column names in DataFrame
    """
    skiprows, header = _skiprows_header_before(doc, '<')
    names = header.split()
    names[0] = names[0] + '*'
    names = names + ['']
    df = pd.read_table(doc, delimiter='|', engine='python', skiprows=skiprows,
                       header=None, names=names, skipfooter=1)\
        .dropna(axis=1, how='all')
    df[names[0]] = df[names[0]].apply(lambda x: int(re.sub(r'^<\s+', '', x)))
    df.set_index(names[0], inplace=True)
    return df


def _skiprows_header_after(doc, mark):
    return_next = False
    for n, r in enumerate(open(doc)):
        if return_next:
            return n, r
        if r[:len(mark)] == mark:             # Found mark!. So break next line
            return_next = True


def _skiprows_header_before(doc, mark):
    for n, r in enumerate(open(doc)):
        if r[:len(mark)] == mark:             # Found mark!. So break next line
            return n, rold
        rold = r


def _widths_skiprows(doc):
    skiprows, header = _skiprows_header_after(doc, '<<')
    w = [m.start() for m in re.finditer(r'\s+', header)]    # Get width columns
    w = [t-s for s, t in zip(w, w[1:])]             # Difference between values
    w[0] = w[0]+1                               # A space more to evit problems
    return w, skiprows


def geo2df(doc, source):
    """
    Return pandas DataFrame from csv Promax geometry file.

    Argumets:
    doc -- csv Promax geometry file
    source -- (SIN, SRF or CDP)
    """
    widths, skiprows = _widths_skiprows(doc)
    df = pd.read_fwf(doc, skiprows=skiprows, widths=widths).dropna(how='all')
    df.set_index('STATION', inplace=True)
    warningmess = 'Warning: source should be \"SRF, SIN or CDP\"'
    print(warningmess) if source not in ('SIN', 'SRF', 'CDP') else None
    df.index.names = ['%sGEO*' % source]
    return df


def promax2meta(doc, target):
    """
    Return meta information (Line or Area) of csv Promax geometry file.

    Arguments:
    doc -- csv Promax geometry file
    target -- meta information to get (Line or Area)
    """
    ptarget = r'' + re.escape(target) + r'\s*[=:]\s*\"?([\w-]+)\"?'
    for line in open(doc):
        result = (re.search(ptarget, line, re.I))
        if result:
            return result.group(1)


def targetInCol(df, target):
    """
    Return meta information (Line or Area) from information in a column of DF.

    Arguments:
    doc -- csv Promax geometry file
    target -- meta information to get (Line or Area)
    """
    c = list(df.columns)
    ptarget = r''+re.escape(target)
    i = [i for i, x in enumerate(c) if re.search(ptarget, x, re.I)]
    return df.iloc[0][i[0]] if i else None


def _chano(ano):
    return '19' + ano if len(ano) == 2 else ano


def normNameLine(name):
    """
    Return normalized name of Line to ANH standars

    Arguments:
    name -- name of Line to normalize
    """
    pline = re.compile(r'^([a-zA-Z]+)[_-]*([0-9]+)[_-]+([0-9]+)')
    search = pline.search(name)
    try:
        name, ano, num = pline.search(name).groups()
    except (AttributeError, TypeError):
        print('Error: Line \"%s\" don\'t match as Line name' % name)
    else:
        return (name + '-' + _chano(ano) + '-' + num).upper()


def normNameArea(name):
    """
    Return normalized name of Area to ANH standars

    Arguments:
    name -- name of Area to normalize
    """
    parea = re.compile(r'^([a-zA-Z]+)[_-]+([0-9]+)')
    try:
        name, ano = parea.search(name).groups()
    except (AttributeError, TypeError):
        print('Error: Area \"%s\" don\'t match as Area name' % name)
    else:
        return (name + '-' + ano).upper()


def addmeta(df, *values):
    """
    update inplace DataFrame with meta information
    Arguments:
    df -- pandas DataFrame to modify
    values -- (colheader, value) to insert in DataFrame
    """
    normName = {
        'Line': normNameLine,
        'Area': normNameArea
    }
    for name, value in values:
        nsname = _chname(name)  # starname = ALGO*, nsname = nostarname = ALGO
        if targetInCol(df, nsname):
            df[nsname.upper()] = \
                normName[nsname.capitalize()](targetInCol(df, nsname))
            df.rename(columns={nsname: name}, inplace=True)
        else:
            df[name] = value


def planesMagna2geo(*planes):
    """
    Return tuple geographycs WGS84 coordinates:
    (lat1, lat2, ...), (lon1, lon2, ...)
    from magna Bogota planes Gauss-Kruger projection coordinates (x, y)

    Arguments:
    planes: Tuple coordinates (x1, x2, ...), (y1, y2, ...)
    """
    # Create instance of projection using EPSG:3116
    # MAGNA-SIRGAS / Colombia Bogota zone
    # (http://spatialreference.org/ref/epsg/3116/)
    # Validated with (Augustin Codazzi, 2004, pag 56-65):
    # http://www.igac.gov.co/wps/wcm/connect/91311780469f77c3aff6bf923ecdf8fe/aspectos+practicos.pdf?MOD=AJPERES
    magna = pyproj.Proj('+proj=tmerc \
        +lat_0=4.596200416666666 +lon_0=-74.07750791666666 \
        +k=1 +x_0=1000000 +y_0=1000000 \
        +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
    return magna(*planes, inverse=True)


def _chtype(x):
    if x == 'float64': return 'double'
    if x == 'int64': return 'int'
    if x == 'O': return 'text'


def _chformat(x):
    if type(x) == np.dtype(float): return float(x)
    if type(x) == np.dtype('int64'): return float(x)
    if type(x) == np.dtype(str): return str(x)


def _chname(x):
    """
    Returns changed name. Removing the last *
    """
    return x[:-1] if x[-1] == '*' else x


def cols(df):
    """
    Returns [(index, type, props), (col1, type1, props1), ...]
    for field names DataFrame in correct format to Microsoft Access DataBase

    Arguments:
    df: DataFrame
    """
    c = df.columns.values.tolist()
    t = list(map(_chtype, df.dtypes.tolist()))
    # Is primary? True if field name ends with *
    p = ['PRIMARY KEY' if x[-1] == '*' else '' for x in c]
    c = list(map(_chname, c))

    return list(zip(
                [_chname(df.index.name)] + c,
                [_chtype(df.index.dtype)] + t,
                ['PRIMARY KEY'] + p
                ))


def createTable(doc, fields, tableName):
    """
    Create table in Microsoft Access DataBase

    Arguments:
    doc: C:\\Route\\To\\FILE.accdb
    fields: field1, field2, ... , fieldn (String of fields sep by commas)
    tableName: Name of Table in Access
    """
    odbc = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % doc
    conn = pyodbc.connect(odbc)
    cursor = conn.cursor()

    try:
        cursor.execute("CREATE TABLE %s(%s)" % (tableName, ','.join(fields)))
        conn.commit()
    except pyodbc.ProgrammingError:
        print('Warning: Table %s already exists' % tableName)

    cursor.close()
    del cursor
    conn.close()


def addPrimaryKeys(doc, primaryKeys, tableName):
    """
    Add Primay keys at table in Microsoft Access DataBase

    Arguments:
    doc: C:\\Users\\Route\\To\\FILE.accdb
    fields: primary1, primary2, ... , primarym (String of fields sep by commas)
    tableName: Name of Table in Access
    """
    odbc = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % doc
    conn = pyodbc.connect(odbc)
    cursor = conn.cursor()
    try:
        cursor.execute("ALTER TABLE %s ADD PRIMARY KEY (%s)"
                       % (tableName, ','.join(primaryKeys)))
        conn.commit()
    except Exception:
        print('Warning: Primary keys (%s) already exits. Not altered table %s'
              % (','.join(primaryKeys), tableName))

    cursor.close()
    del cursor
    conn.close()


def to_access(doc, df, tableName=None):
    """
    Save Pandas DataFrame to Microsoft Access DataBase

    Arguments:
    doc: C:\\Users\\Route\\To\\FILE.accdb
    df: Pandas DataFrame
    tableName: Name of access table (optional: name of first column)
    """
    ctp = cols(df)                    # (Col,type, isPrimaryKey) list of tuples
    c = [c for c, t, p in ctp]        # Only cols list
    fields = [c + ' ' + t for c, t, p in ctp]
    pk = [c for c, t, p in ctp if p == 'PRIMARY KEY']  # prim keys
    tableName = c[0] if not tableName else tableName
    print('Table %s created in access' % tableName)

    createTable(doc, fields, tableName)
    addPrimaryKeys(doc, pk, tableName)

    # Insert DataFrame
    odbc = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % doc
    conn = pyodbc.connect(odbc)
    cursor = conn.cursor()

    sql_ins = "INSERT INTO %s(%s) VALUES (%s)" \
        % (tableName, ','.join(c), ','.join(['?']*len(c)))

    for row in df.itertuples():
        try:
            cursor.execute(sql_ins, tuple(map(_chformat, row)))
            conn.commit()
        except pyodbc.IntegrityError:
            print('%s = %s already exists. Not inserted' % (c[0], row[0]))

    cursor.close()
    del cursor
    conn.close()


def main():
    workfolder = 'C:\\Users\\Usuario9\\Desktop\\JOHN\\'
    ACCESS_DATABASE_FILE = workfolder + '1.ANH_SISMICA\\DOC_SISMICA\\GEODATABASE2.accdb'
    datafolder = workfolder + '2.INSUMOS\\l-82-9600\\'
    SIN_PROMAX_FILE = datafolder + 'l-82-9600-sin.a_db'
    SRF_PROMAX_FILE = datafolder + 'l-82-9600-srf.a_db'
    CDP_PROMAX_FILE = datafolder + 'l-82-9600-cdp.a_db'
    # GEO_UKOOA_FILE = datafolder + '829700.geo'

    line = normNameLine(promax2meta(SIN_PROMAX_FILE, 'Line'))
    area = normNameArea(promax2meta(SIN_PROMAX_FILE, 'Area'))

    cdp = promaxdb2df(CDP_PROMAX_FILE)
    sin = promaxdb2df(SIN_PROMAX_FILE)
    srf = promaxdb2df(SRF_PROMAX_FILE)
    # geo = geo2df(GEO_UKOOA_FILE, 'SIN')

    addmeta(cdp, ('LINE*', line), ('AREA*', area))
    addmeta(sin, ('LINE*', line), ('AREA*', area))
    addmeta(srf, ('LINE*', line), ('AREA*', area))
    # addmeta(geo, ('LINE*', line), ('AREA*', area))

    sin['LONGITUDE'], sin['LATITUDE'] = planesMagna2geo(
        list(sin['X_COORD']), list(sin['Y_COORD']))
    srf['LONGITUDE'], srf['LATITUDE'] = planesMagna2geo(
        list(srf['X_COORD']), list(srf['Y_COORD']))
    cdp['LONGITUDE'], cdp['LATITUDE'] = planesMagna2geo(
        list(cdp['X_COORD']), list(cdp['Y_COORD']))

    print(sin, srf, cdp)

    to_access(ACCESS_DATABASE_FILE, cdp)
    to_access(ACCESS_DATABASE_FILE, sin)
    to_access(ACCESS_DATABASE_FILE, srf)
    # to_access(ACCESS_DATABASE_FILE, geo)


if __name__ == "__main__":
    main()

    # change point decimal by comma and Save to ascii
    # cdp.applymap(str).replace(r'\.', ',', regex=True).to_csv('csv', sep=';')
