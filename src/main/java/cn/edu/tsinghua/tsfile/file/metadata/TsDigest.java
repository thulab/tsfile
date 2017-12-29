package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.format.Digest;

import java.util.HashMap;
import java.util.Map;

/**
 * For more information, see Digest in cn.edu.thu.tsfile.format package
 */
public class TsDigest implements IConverter<Digest> {
	/**
	 * Digest/statistics per row group and per page.
	 */
	public Map<String, String> statistics;

	public TsDigest() {
	}

	public TsDigest(Map<String, String> statistics) {
		this.statistics = statistics;
	}
	
	public void setStatistics(Map<String, String> statistics) {
		this.statistics = statistics;
	}
	
	public Map<String, String> getStatistics(){
		return this.statistics;
	}
	
	public void addStatistics(String key, String value) {
		if(statistics == null) {
			statistics = new HashMap<>();
		}
		statistics.put(key, value);
	}

	@Override
	public String toString() {
		return statistics != null ? statistics.toString() : "";
	}

	@Override
	public Digest convertToThrift() {
		Digest digest = new Digest();
		if (statistics != null) {
			Map<String, String> statisticsInThrift = new HashMap<>();
			for (String key : statistics.keySet()) {
				statisticsInThrift.put(key, statistics.get(key));
			}
			digest.setStatistics(statisticsInThrift);
		}
		return digest;
	}

	@Override
	public void convertToTSF(Digest digestInThrift) {
		if (digestInThrift != null) {
			Map<String, String> statisticsInThrift = digestInThrift.getStatistics();
			if (statisticsInThrift != null) {
				statistics = new HashMap<>();
				for (String key : statisticsInThrift.keySet()) {
					statistics.put(key, statisticsInThrift.get(key));
				}
			} else {
				statistics = null;
			}
		}
	}
}
