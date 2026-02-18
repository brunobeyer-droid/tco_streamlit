import React from "react";
import { createRoot } from "react-dom/client";
import { Streamlit, withStreamlitConnection } from "streamlit-component-lib";
import { ErrorBoundary } from "./ErrorBoundary";
import { App } from "./App";
import "./styles/roadmap.css";

const Connected = withStreamlitConnection((props: any) => {
  const data = (props.args as any)?.data || { pis: [], apps: [], cells: {}, meta: {} };
  const height = (props.args as any)?.height || 850;

  return (
    <ErrorBoundary>
      <App data={data} height={height} />
    </ErrorBoundary>
  );
});

const root = createRoot(document.getElementById("root")!);
root.render(<Connected />);
