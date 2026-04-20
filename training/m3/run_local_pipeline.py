import subprocess
import sys


def run_step(cmd: list[str], title: str):
    print("\n" + "=" * 60)
    print(f"STEP: {title}")
    print("=" * 60)

    result = subprocess.run(cmd, text=True)

    if result.returncode != 0:
        print(f"\n❌ FAILED: {title}")
        sys.exit(result.returncode)

    print(f"\n✅ SUCCESS: {title}")


def main(user_id: str):
    # 1. simulate production data
    run_step(
        [
            "python",
            "training/m3/simulate_production_data.py",
            "--user-id",
            user_id,
            "--months",
            "6",
            "--include-forecast-requests",
        ],
        "Simulate production traffic",
    )

    # 2. build monthly history
    run_step(
        ["python", "training/m3/build_local_monthly_history.py"],
        "Build local monthly history",
    )

    # 3. build training rows
    run_step(
        ["python", "training/m3/build_local_training_rows.py"],
        "Build local training rows",
    )

    # 4. train local model
    run_step(
        ["python", "training/m3/train_local_user_model.py", user_id],
        "Train local candidate model",
    )

    # 5. compare and promote
    run_step(
        ["python", "training/m3/compare_and_promote_local_model.py", user_id],
        "Compare & promote decision",
    )

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python training/m3/run_local_pipeline.py <user_id>")
        sys.exit(1)

    main(sys.argv[1])