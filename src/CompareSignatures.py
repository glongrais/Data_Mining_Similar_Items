

class CompareSignatures:
   
    def __init__(self, sigMatrix):
        self.sigMatrix = sigMatrix
        self.compare()

    def compare(self):
        rows =len(self.sigMatrix)
        cols = len(self.sigMatrix[0])
        simMatrix = []
        for i in range(cols):
            tmp = []
            for j in range(cols):
                tmp.append(0)
            simMatrix.append(tmp)

        for i in range(rows):
            for j in range(cols):
                for u in range(cols):
                    if self.sigMatrix[i][j]==self.sigMatrix[i][u]:
                        simMatrix[u][j] += 1
        print()
        simMatrix = [[(j/rows) for j in i] for i in simMatrix]
        for i in range(cols):
            print(simMatrix[i])
