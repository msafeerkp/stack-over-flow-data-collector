package org.stackoverflow.data.collector.model;

public class MetaModel {
	
	private long pageNumber;
	private long dayCount;
	private long time;
	private String metaType;
	
	public MetaModel(MetaModel metaModel) {
		
		this.pageNumber = metaModel.getPageNumber();
		this.dayCount = metaModel.getDayCount();
		this.time = metaModel.getTime();
		this.metaType = metaModel.getMetaType();
		
	}
	
	public MetaModel() {}
	
	public long getPageNumber() {
		return pageNumber;
	}
	public void setPageNumber(long pageNumber) {
		this.pageNumber = pageNumber;
	}
	public long getDayCount() {
		return dayCount;
	}
	public void setDayCount(long dayCount) {
		this.dayCount = dayCount;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	public String getMetaType() {
		return metaType;
	}
	public void setMetaType(String metaType) {
		this.metaType = metaType;
	}
	
	@Override
	public String toString() {
		return "PostMetaModel [pageNumber=" + pageNumber + ", dayCount=" + dayCount + ", time=" + time + ", metaType="
				+ metaType + "]";
	}

}
