import torch
import torch.nn as nn


class TinyM1Model(nn.Module):
    def __init__(self):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(4, 16),
            nn.ReLU(),
            nn.Linear(16, 3)
        )

    def forward(self, x):
        return self.net(x)


def main():
    model = TinyM1Model()
    model.eval()

    dummy_input = torch.randn(1, 4)

    torch.onnx.export(
        model,
        dummy_input,
        "model.onnx",
        input_names=["features"],
        output_names=["logits"],
        dynamic_axes={
            "features": {0: "batch_size"},
            "logits": {0: "batch_size"}
        },
        opset_version=12
    )

    print("Exported model.onnx")


if __name__ == "__main__":
    main()
