import React from "react";
import { RoadmapItem } from "../types";

type Labels = {
  completed?: string;
  inProgress?: string;
  validation?: string;
  planned?: string;
};

type FeatureModalProps = {
  item: RoadmapItem;
  appLabel: string;
  piLabel: string;
  labels: Labels;
  anchorTop: number;
  onClose: () => void;
};

const statusLabel = (bucket: string, labels: Labels) => {
  const normalized = bucket.toLowerCase();
  if (normalized === "done") return labels.completed || "Completed";
  if (normalized === "active") return labels.inProgress || "In Progress";
  if (normalized === "validation") return labels.validation || "Validation";
  return labels.planned || "Planned";
};

const statusToken = (bucket: string) => {
  const normalized = bucket.toLowerCase();
  if (normalized === "done") return "done";
  if (normalized === "active") return "active";
  if (normalized === "validation") return "validation";
  return "planned";
};

export function FeatureModal({ item, appLabel, piLabel, labels, anchorTop, onClose }: FeatureModalProps) {
  const progressPct = Number(item.progress_pct || 0);
  const hasProgress = Number(item.progress_total_sp || 0) > 0;
  const epicProgressPct = Number(item.epic_progress_pct || 0);
  const epicFeatureTotal = Number(item.epic_feature_count_total || 0);
  const epicFeatureWithStories = Number(item.epic_feature_count_with_stories || 0);
  const epicFeatureWithSp = Number(item.epic_feature_count_with_sp || 0);
  const hasEpicFeatureCounts =
    epicFeatureTotal > 0 || epicFeatureWithStories > 0 || epicFeatureWithSp > 0;
  return (
    <div className="roadmap-modal-backdrop" onClick={onClose}>
      <div
        className="roadmap-modal"
        style={{ top: "50%", left: "50%", transform: "translate(-50%, -50%)" }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="roadmap-modal-header">
          <div>
            <div className="roadmap-modal-title">{item.title}</div>
            <div className="roadmap-modal-sub">
              {appLabel} · {piLabel}
            </div>
          </div>
          <button className="roadmap-icon-button" onClick={onClose}>
            ✕
          </button>
        </div>

        <div className="roadmap-modal-badges">
          <span className={`roadmap-status roadmap-status-${statusToken(item.statusBucket)}`.trim()}>
            {statusLabel(item.statusBucket, labels)}
          </span>
        </div>

        <div className="roadmap-modal-grid">
          <div>
            <div className="roadmap-modal-label">Program</div>
            <div>{item.program || "—"}</div>
          </div>
          <div>
            <div className="roadmap-modal-label">Team</div>
            <div>{item.team || "—"}</div>
          </div>
          <div>
            <div className="roadmap-modal-label">State</div>
            <div>{item.state || "—"}</div>
          </div>
          <div>
            <div className="roadmap-modal-label">Mapping</div>
            <div>{item.mapping_status || "—"}</div>
          </div>
        </div>

        <div className="roadmap-modal-metrics">
          <div>
            <div className="roadmap-modal-label">Story Points</div>
            <div>{Number(item.points || 0).toLocaleString()}</div>
          </div>
          <div>
            <div className="roadmap-modal-label">Derived FTE</div>
            <div>{Number(item.fte || 0).toFixed(2)}</div>
          </div>
          <div>
            <div className="roadmap-modal-label">Progress</div>
            <div>{hasProgress ? `${Math.round(progressPct * 100)}%` : "—"}</div>
          </div>
        </div>
        {hasProgress && (
          <div className="roadmap-modal-grid">
            <div>
              <div className="roadmap-modal-label">Proposed SP</div>
              <div>{Number(item.progress_proposed_sp || 0).toLocaleString()}</div>
            </div>
            <div>
              <div className="roadmap-modal-label">In Progress SP</div>
              <div>{Number(item.progress_inprogress_sp || 0).toLocaleString()}</div>
            </div>
            <div>
              <div className="roadmap-modal-label">Completed SP</div>
              <div>{Number(item.progress_completed_sp || 0).toLocaleString()}</div>
            </div>
            <div>
              <div className="roadmap-modal-label">Total SP</div>
              <div>{Number(item.progress_total_sp || 0).toLocaleString()}</div>
            </div>
          </div>
        )}

        {(item.epic_title || item.epic_state) && (
          <div className="roadmap-modal-grid">
            <div>
              <div className="roadmap-modal-label">Epic</div>
              <div>{item.epic_title || "—"}</div>
            </div>
            <div>
              <div className="roadmap-modal-label">Epic State</div>
              <div>{item.epic_state || "—"}</div>
            </div>
            <div>
              <div className="roadmap-modal-label">Epic Progress</div>
              <div>{Number.isFinite(epicProgressPct) ? `${Math.round(epicProgressPct * 100)}%` : "—"}</div>
            </div>
          </div>
        )}

        {hasEpicFeatureCounts && (
          <div className="roadmap-modal-grid">
            <div>
              <div className="roadmap-modal-label">Features</div>
              <div>{Number(epicFeatureTotal || 0).toLocaleString()}</div>
            </div>
            <div>
              <div className="roadmap-modal-label">With Stories</div>
              <div>
                {Number(epicFeatureWithStories || 0).toLocaleString()}/
                {Number(epicFeatureTotal || 0).toLocaleString()}
              </div>
            </div>
            <div>
              <div className="roadmap-modal-label">With SP</div>
              <div>
                {Number(epicFeatureWithSp || 0).toLocaleString()}/
                {Number(epicFeatureTotal || 0).toLocaleString()}
              </div>
            </div>
          </div>
        )}

        <div className="roadmap-modal-actions">
          {item.feature_url && (
            <a
              className="roadmap-button"
              href={item.feature_url}
              target="_blank"
              rel="noreferrer"
            >
              Open Feature in ADO
            </a>
          )}
          {item.epic_url && (
            <a
              className="roadmap-button"
              href={item.epic_url}
              target="_blank"
              rel="noreferrer"
            >
              Open Epic in ADO
            </a>
          )}
          <button className="roadmap-button" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
