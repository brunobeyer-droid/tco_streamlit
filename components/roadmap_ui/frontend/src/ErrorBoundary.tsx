import React from "react";
import { Streamlit } from "streamlit-component-lib";

type Props = { children: React.ReactNode };
type State = { hasError: boolean; message?: string; stack?: string };

export class ErrorBoundary extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(err: any) {
    return { hasError: true, message: String(err?.message || err) };
  }

  componentDidCatch(err: any) {
    const message = String(err?.message || err);
    const stack = String(err?.stack || "");
    // Surface errors to console for devs.
    // eslint-disable-next-line no-console
    console.error("Roadmap UI error:", err);
    Streamlit.setComponentValue({ type: "error", message, stack });
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="roadmap-error-panel">
          <div className="roadmap-error-panel-title">Roadmap UI error</div>
          <div className="roadmap-error-panel-text">
            {this.state.message || "Something went wrong while rendering the roadmap."}
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
