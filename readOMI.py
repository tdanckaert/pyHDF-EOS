import pyhdf
import pyhdf.HDF as HDF
import pyhdf.SD as SD
import pyhdf.V as V
import pyhdf.VS as VS
import pyhdf.HC as HC
import weakref

def close_refs(set):
    """Remove all elements from a set and call the "close()" method on
them."""
    while len(set):
        # Iterating over the set might be unsafe, because elements
        # could disappear due to garbage collection.  Therefore we
        # use repeated pop()'s until a KeyError is thrown:
        set.pop().close()

class HDFEOS:

    def __init__(self, filename):
        self.filename = filename
        self._file = HDF.HDF(self.filename)
        self._v=self._file.vgstart()
        self._vs=self._file.vstart()
        self._sd=SD.SD(filename)
        self._open=True
        self._swathrefs=weakref.WeakSet() # Weak references to opened swaths
        self._swathdict = {}
        ref= -1
        try:
            while True:
                ref = self._v.getid(ref)
                with vgroup(self, ref) as group:
                    if group._vg._class == 'SWATH':
                        self._swathdict[group._vg._name] = ref
        except pyhdf.error.HDF4Error:
            pass

    def close(self):
      """
      @brief Close the file.  Any swaths, variables or groups from this file will be closed as well.
      :param self: p_self:...
      :type self: t_self:HDFEOS
      :returns: r:...
      """
      if self._open:
            close_refs(self._swathrefs)
            self._v.end()
            self._vs.vend()
            self._sd.end()
            self._file.close()
            self._open=False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def swaths(self):
      """
      @brief Get the set of the names of all swaths in this file.
      :param self: p_self:...
      :type self: t_self:HDFEOS
      :returns: r:...
      """
      return self._swathdict.keys()

    def __getitem__(self,name):
        s=swath(self, self._v.attach(self._swathdict[name]))
        self._swathrefs.add(s)
        return s

class swath:
    """A HDFEOS swath.

    Attributes:
    data: the vgroup 'Data Fields'
    geolocation: the vgroup 'Geolocation Fields'
    attributes: the vgroup 'Swath Attributes'
    """

    def __init__(self, file, vg):
        self._open = True
        for tag, ref in vg.tagrefs():
            if tag == HC.HC.DFTAG_VG:
                group = vgroup(file, ref)
                if group._vg._name == 'Data Fields':
                    self.data = group
                elif group._vg._name == 'Geolocation Fields':
                    self.geolocation = group
                elif group._vg._name == 'Swath Attributes':
                    self.attributes = group
                else: # normally, a HDF-EOS swath should only contain above 3 groups, but let's be safe
                    group.close()

    def close(self):
        """
        @brief Close the swath.  Variables created from this swath can not be accessed anymore after closing the swath.
        :param self: p_self:...
        :type self: t_self:swath
        :returns: r:...
        """
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
        self._open=True
        self.name=self._vg._name
        self._tagrefdict=None
        self._itemrefs=weakref.WeakSet() # Weak references to opened items.

    # The different types of objects are indentified by their tags =>
    # look up correct constructor depending on the tag:
    dispatch = {
        HC.HC.DFTAG_VH : vdata,
        HC.HC.DFTAG_NDG : sd,
        HC.HC.DFTAG_VG : __init__
    }

    def close(self):
        if self._open:
            close_refs(self._itemrefs)
            self._vg.detach()
            self._open=False

    def __enter__(self):
        return self

    def _tagrefs(self):
        if not self._tagrefdict:
            # lazily build _tagrefdict:
            self._tagrefdict = {}
            for tag, ref in self._vg.tagrefs():
                if tag in self.dispatch: # we only care for vdata, sd or vgroup objects
                    with self.dispatch[tag](self._file, ref) as entry:
                        self._tagrefdict[entry.name] = [tag,ref]
        return self._tagrefdict

    def content(self):
        """
        @brief Get a set (dict.keys()) with the names of all variables or subgroups in this group.
        :param self: p_self:...
        :type self: t_self:vgroup
        :returns: r:...
        """
        return self._tagrefs().keys()

    def __exit__(self, *args):
        self.close()

    def __getitem__(self, key):
        tag, ref = self._tagrefs()[key]
        item=self.dispatch[tag](self._file, ref)
        self._itemrefs.add(item)
        return item
