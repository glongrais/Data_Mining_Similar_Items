jaccard:
	python3 src/Main.py jaccard ${NGRAM}

minHashing:
	python3 src/Main.py minHashing ${NGRAM} ${NBHASH} ${THRESHOLD} 

LSH:
	python3 src/Main.py LSH ${NGRAM} ${NBHASH} ${THRESHOLD} ${NBBAND}
