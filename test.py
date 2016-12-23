import readOMI

print("Open file:")
with readOMI.HDFEOS('/home/thomasd/Testdata/OMI/L1BRUG-2007-166/OMI-Aura_L1-OML1BRUG_2007m0615t0133-o15506_v003-2011m0126t111525-p1.he4') as testfile:
    print(testfile.swaths)
    with testfile.openswath('Earth UV-1 Swath') as swath:
        print("Swath:", swath.data, swath.geolocation, swath.attributes)
        print("Data Fields:")
        with swath.geolocation['Time'] as field:
            print(field.name, field.size)

print("Done using file.")    
