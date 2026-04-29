package org.esgi.project.streaming.models;

public final class ViewStats {

    public String title;
    public long totalViews;
    public long startOnly;
    public long half;
    public long full;

    public ViewStats() {
    }

    public ViewStats add(View view) {
        this.title = view.title();
        this.totalViews++;
        switch (view.category()) {
            case START_ONLY -> this.startOnly++;
            case HALF -> this.half++;
            case FULL -> this.full++;
        }
        return this;
    }

    public ViewStats merge(ViewStats other) {
        if (other == null) {
            return this;
        }
        if (this.title == null) {
            this.title = other.title;
        }
        this.totalViews += other.totalViews;
        this.startOnly += other.startOnly;
        this.half += other.half;
        this.full += other.full;
        return this;
    }
}
