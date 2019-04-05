package jheister.lmdbcollections;

import java.util.Objects;

public class Stats {
    public final String name;
    public final long branchPages;
    public final int depth;
    public final long entries;
    public final long leafPages;
    public final long overflowPages;
    public final int pageSize;

    public Stats(String name,
                 long branchPages,
                 int depth,
                 long entries,
                 long leafPages,
                 long overflowPages,
                 int pageSize) {
        this.name = name;
        this.branchPages = branchPages;
        this.depth = depth;
        this.entries = entries;
        this.leafPages = leafPages;
        this.overflowPages = overflowPages;
        this.pageSize = pageSize;
    }

    public long size() {
        return leafSize() + branchSize() + overflowSize();
    }

    public long overflowSize() {
        return overflowPages * pageSize;
    }

    public long leafSize() {
        return leafPages * pageSize;
    }

    public long branchSize() {
        return branchPages * pageSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Stats stats = (Stats) o;
        return branchPages == stats.branchPages &&
                depth == stats.depth &&
                entries == stats.entries &&
                leafPages == stats.leafPages &&
                overflowPages == stats.overflowPages &&
                pageSize == stats.pageSize &&
                Objects.equals(name, stats.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, branchPages, depth, entries, leafPages, overflowPages, pageSize);
    }

    @Override
    public String toString() {
        return "Stats{" +
                "name='" + name + '\'' +
                ", branchPages=" + branchPages +
                ", depth=" + depth +
                ", entries=" + entries +
                ", leafPages=" + leafPages +
                ", overflowPages=" + overflowPages +
                ", pageSize=" + pageSize +
                '}';
    }
}
