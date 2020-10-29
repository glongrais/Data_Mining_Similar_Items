class Shingling:

    def __init__(self, name, k):
        self.doc = name
        self.k   = k
        print(self.doc)




cl = Shingling("../Datas/accuracy_garmin_nuvi_255W_gps.txt",4)
#f = open("../Datas/accuracy_garmin_nuvi_255W_gps.txt", "r")
#print(f.read()) 