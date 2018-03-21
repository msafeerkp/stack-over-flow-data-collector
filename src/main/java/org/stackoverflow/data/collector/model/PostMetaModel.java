package org.stackoverflow.data.collector.model;

public class PostMetaModel {
	
	private long pageNumber;
	private long dayCount;
	private long time;
	
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
	
	@Override
	public String toString() {
		return "PostMetaModel [pageNumber=" + pageNumber + ", dayCount=" + dayCount + ", time=" + time + "]";
	}

	
}
