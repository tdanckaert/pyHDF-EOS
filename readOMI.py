import pyhdf
import pyhdf.HDF as HDF
import pyhdf.SD as SD
import pyhdf.V as V
import pyhdf.VS as VS
import pyhdf.HC as HC

class HDFEOS:

    def __init__(self, filename):
        self.filename = filename
        self.file = HDF.HDF(self.filename)
        self.v=self.file.vgstart()
        self.vs=self.file.vstart()
        self.sd=SD.SD(filename)

        self.swaths = self.listswaths()

    def close(self):
        self.v.end()
        self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def listswaths(self):
        swaths = {}
        ref= -1
        try:
            while True:
                ref = self.v.getid(ref)
                with vgroup(self, ref) as group:
                    if group.vg._class == 'SWATH':
                        swaths[group.vg._name] = ref
        except pyhdf.error.HDF4Error:
            return swaths

    def openswath(self,name):
        return swath(self, self.v.attach(self.swaths[name]))

class swath:

    def __init__(self, file, vg):
        self.file = file
        self.vg = vg
        self.open = True
        for tag, ref in vg.tagrefs():
            if tag == HC.HC.DFTAG_VG:
                group = vgroup(self.file, ref)
                if group.vg._name == 'Data Fields':
                    self.data = group
                elif group.vg._name == 'Geolocation Fields':
                    self.geolocation = group
                elif group.vg._name == 'Swath Attributes':
                    self.attributes = group
                else: # normally, a HDF-EOS swath should only contain above 3 groups, but let's be safe
                    group.close()

    def close(self):
        if self.open:
            self.data.close()
            self.geolocation.close()
            self.attributes.close()
            self.open = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

class vdata:

    def __init__(self,file, ref):
        self.vd=file.vs.attach(ref)
        self.nrecs, self.intmode, self.fields, self.size, self.name = self.vd.inquire()
        self.open = True

    def close(self):
        if self.open:
            self.vd.detach()
            self.open = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        print("delete vdata", self.name)
        self.close()

    def read(self, *args):
        return self.vd.read(*args)

class sd:

    def __init__(self,file,ref):
        self.sds=file.sd.select(file.sd.reftoindex(ref))
        self.name, self.rank, self.dims, self.type, self.nattrs = self.sds.info()
        self.open = True

    def close(self):
        if self.open:
            self.sds.endaccess()
            self.open = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        print("delete sd", self.name)
        self.close()

    def get(self, *args):
        return self.sds.get(*args)

class vgroup:

    def __init__(self, file, ref):
        self.file=file
        self.vg=file.v.attach(ref)
        self.name=self.vg._name
        self.contents=None

    def close(self):
        self.vg.detach()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    dispatch = {
        HC.HC.DFTAG_VH : vdata,
        HC.HC.DFTAG_NDG : sd,
        HC.HC.DFTAG_VG : __init__
    }

    def _build_index(self):
        self.contents = {}
        for tag, ref in self.vg.tagrefs():
            if tag in self.dispatch:
                with self.dispatch[tag](self.file, ref) as entry:
                    self.contents[entry.name] = [tag,ref]

    def __getitem__(self, key):
        if not self.contents:
            self._build_index()
        tag, ref = self.contents[key]
        return self.dispatch[tag](self.file, ref)
