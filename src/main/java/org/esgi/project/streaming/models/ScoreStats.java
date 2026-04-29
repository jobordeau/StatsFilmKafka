package org.esgi.project.streaming.models;

public final class ScoreStats {

    public double totalScore;
    public long count;

    public ScoreStats() {
    }

    public ScoreStats add(double score) {
        this.totalScore += score;
        this.count++;
        return this;
    }

    public double average() {
        return count > 0 ? totalScore / count : 0.0;
    }
}
