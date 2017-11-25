package word_kmeans111;

import java.util.Set;

public interface Instance {
	public double distance(Instance inst);
	public Instance centroid(Set insts);
	public Instance centroid(Instance... insts);
	public boolean isSamePoint(Instance inst);
}
