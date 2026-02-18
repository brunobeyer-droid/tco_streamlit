import React from "react";
import { RoadmapBoard } from "./RoadmapBoard";
import { RoadmapData } from "./types";

export function App({ data, height }: { data: RoadmapData; height: number }) {
  return (
    <div className="roadmap-root">
      <RoadmapBoard data={data} height={height} />
    </div>
  );
}
