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
                with vgroup(self, self.v.attach(ref)) as group:
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
        for tag, ref in vg.tagrefs():
            if tag == HC.HC.DFTAG_VG:
                group = vgroup(self.file, self.file.v.attach(ref))
                if group.vg._name == 'Data Fields':
                    self.data = group
                elif group.vg._name == 'Geolocation Fields':
                    self.geolocation = group
                elif group.vg._name == 'Swath Attributes':
                    self.attributes = group
                else: # normally, a HDF-EOS swath should only contain above 3 groups, but let's be safe
                    group.close()

    def close(self):
        self.data.close()
        self.geolocation.close()
        self.attributes.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

class vdata:

    def __init__(self,file, ref):
        self.vd=file.vs.attach(ref)
        self.nrecs, self.intmode, self.fields, self.size, self.name = self.vd.inquire()

    def close(self):
        self.vd.detach()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

class sd:

    def __init__(self,file,ref):
        self.sds=file.sd.select(file.sd.reftoindex(ref))
        self.name, self.rank, self.dims, self.type, self.nattrs = self.sds.info()

    def close(self):
        self.sds.endaccess()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

class vgroup:

    def open_vgroup(file, ref):
        return vgroup(file, file.v.attach(ref))

    dispatch = {
        HC.HC.DFTAG_VH : vdata,
        HC.HC.DFTAG_NDG : sd,
        HC.HC.DFTAG_VG : open_vgroup
    }

    def __init__(self, file, vg):
        self.file=file
        self.name=vg._name
        self.vg=vg
        self.contents = {}
        for tag, ref in self.vg.tagrefs():
            if tag in self.dispatch:
                with self.dispatch[tag](self.file, ref) as entry:
                    self.contents[entry.name] = [tag,ref]

    def close(self):
        self.vg.detach()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __getitem__(self, key):
        tag, ref = self.contents[key]
        return self.dispatch[tag](self.file, ref)
