package word_kmeans;


import java.util.HashSet;
import java.util.Set;

public class WordInst{
	public String word = null;
	public Utils utils = new Utils();

	public WordInst(){};

	public WordInst(String word) {
		this.word = word;
	}

	public double distance(WordInst inst) {
		return utils.getDistance(this, inst);
	}


	public WordInst centroid(WordInst... insts) {
		Set set = new HashSet();
		for (WordInst inst:insts)
			set.add(inst);
		set.add(this);
		return this.centroid(set);
	}

	public synchronized WordInst centroid(Set<WordInst> insts) {

		double sumSimilarity = 0;
        double maxSimilarity = 0;

        String word = null;
        for (WordInst inst:insts) {
            sumSimilarity = 0;
			for (Object yinst:insts) {
				sumSimilarity += utils.getDistance(inst, ((WordInst)yinst));
			}
            System.out.println("\t"+sumSimilarity);
			if (maxSimilarity < sumSimilarity) {
				maxSimilarity = sumSimilarity;
				word = inst.word;
			}
        }
        return new WordInst(word);
	}

	public String toString(){
		return String.format("Inst[word=%s;]", this.word);
	}

	public boolean isSamePoint(WordInst inst) {
		if (this.word == inst.word)
			return true;
		return false;
	}
}
