import pyhdf
import pyhdf.HDF as HDF
import pyhdf.SD as SD
import pyhdf.V as V
import pyhdf.VS as VS
import pyhdf.HC as HC

class HDFEOS:

    def __init__(self, filename):
        self.filename = filename
        self._file = HDF.HDF(self.filename)
        self._v=self._file.vgstart()
        self._vs=self._filevstart()
        self._sd=SD.SD(filename)

        self.swaths = self.listswaths()

    def close(self):
        self._v.end()
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def listswaths(self):
        swaths = {}
        ref= -1
        try:
            while True:
                ref = self._v.getid(ref)
                with vgroup(self, ref) as group:
                    if group._vg._class == 'SWATH':
                        swaths[group._vg._name] = ref
        except pyhdf.error.HDF4Error:
            return swaths

    def openswath(self,name):
        return swath(self, self._v.attach(self.swaths[name]))

class swath:

    def __init__(self, file, vg):
        self._file = file
        self._vg = vg
        self._open = True
        for tag, ref in vg.tagrefs():
            if tag == HC.HC.DFTAG_VG:
                group = vgroup(self._file, ref)
                if group._vg._name == 'Data Fields':
                    self.data = group
                elif group._vg._name == 'Geolocation Fields':
                    self.geolocation = group
                elif group._vg._name == 'Swath Attributes':
                    self.attributes = group
                else: # normally, a HDF-EOS swath should only contain above 3 groups, but let's be safe
                    group.close()

    def close(self):
        if self._open:
            self.data.close()
            self.geolocation.close()
            self.attributes.close()
            self._open = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

class vdata:

    def __init__(self,file, ref):
        self._vd=file._vs.attach(ref)
        self.nrecs, self.intmode, self.fields, self.size, self.name = self._vd.inquire()
        self._open = True

    def close(self):
        if self._open:
            self._vd.detach()
            self._open = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    def __getitem__(self, indexes):
        if isinstance(indexes, slice):
            # python-hdf4 VD objects implement Python slicing, but
            # ignore the ``stride'' or ``step'' argument, so we handle
            # that ourselves by getting the slice without stride
            # first, and slicing it again using the requested stride
            # (this could be a little wasteful because we retrieve
            # elements and then throw them away).
            return self._vd.__getitem__(indexes)[::indexes.step]
        else:
            return self._vd.__getitem__(indexes)

class sd:

    def __init__(self,file,ref):
        self._sds=file._sd.select(file._sd.reftoindex(ref))
        self.name, self.rank, self.dims, self.type, self.nattrs = self._sds.info()
        self._open = True

    def close(self):
        if self._open:
            self._sds.endaccess()
            self._open = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    def __getitem__(self,arg):
        return self._sds.__getitem__(arg)

class vgroup:

    def __init__(self, file, ref):
        self._file=file
        self._vg=file._v.attach(ref)
        self.name=self._vg._name
        self._contents=None

    def close(self):
        self._vg.detach()

    def __enter__(self):
        return self

    def items(self):
        if not self._contents:
            self._build_index()
        return self._contents

    def __exit__(self, *args):
        self.close()

    dispatch = {
        HC.HC.DFTAG_VH : vdata,
        HC.HC.DFTAG_NDG : sd,
        HC.HC.DFTAG_VG : __init__
    }

    def _build_index(self):
        self._contents = {}
        for tag, ref in self._vg.tagrefs():
            if tag in self.dispatch:
                with self.dispatch[tag](self._file, ref) as entry:
                    self._contents[entry.name] = [tag,ref]

    def __getitem__(self, key):
        tag, ref = self.items()[key]
        return self.dispatch[tag](self._file, ref)
