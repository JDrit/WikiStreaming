package edu.rit.csh.github

import edu.rit.csh.github.ActionType.ActionType
import edu.rit.csh.github.EventType.EventType
import edu.rit.csh.github.IssueType.IssueType
import edu.rit.csh.github.RefType.RefType

import org.json4s._
import org.json4s.native.JsonParser._

abstract class GithubEvent(val userName: String)
case class CommitCommentGithubEvent(override val userName: String, repoName: String, desc: String) extends GithubEvent(userName)
case class CreateGithubEvent(override val userName: String, repoName: String, url: String, refType: RefType, desc: String) extends GithubEvent(userName)
case class DeleteGithubEvent(override val userName: String, repoName: String, refTypr: RefType) extends GithubEvent(userName)
case class DeploymentGithubEvent(override val userName: String, repoName: String, url: String) extends GithubEvent(userName)
case class DeploymentStatusGithubEvent(override val userName: String, repoName: String, statusUrl: String) extends GithubEvent(userName)
case class ForkGithubEvent(override val userName: String, repoName: String, ownerName: String, url: String) extends GithubEvent(userName)
case class GollomGithubEvent(override val userName: String, repoName: String, pages: Seq[String]) extends GithubEvent(userName)
case class IssueCommandGithubEvent(override val userName: String, title: String, repoName: String, issueUrl: String) extends GithubEvent(userName)
case class IssuesGithubEvent(override val userName: String, repoName: String, issueType: IssueType, issueUrl: String) extends GithubEvent(userName)
case class MemberGithubEvent(override val userName: String, repoName: String, repoOwner: String, repoUrl: String) extends GithubEvent(userName)
case class MemberShipGithubEvent(override val userName: String, action: ActionType, orgName: String) extends GithubEvent(userName)
case class PageBuildGithubEvent(override val userName: String, buildUrl: String, repoName: String) extends GithubEvent(userName)
case class PublicGithubEvent(override val userName: String, repoName: String, repoUrl: String) extends GithubEvent(userName)
case class PullRequestGithubEvent(override val userName: String, pullUrl: String, message: String, repoName: String) extends GithubEvent(userName)
case class PullRequestReviewGithubEvent(override val userName: String, message: String, repoName: String, ownerName: String) extends GithubEvent(userName)

case class Event(id: Long, username: String, eventType: EventType)

private case class EventParse(id: String, `type`: String)

object EventType extends Enumeration {
  type EventType = Value
  val CommitCommentEvent, CreateEvent, DeleteEvent, DeploymentEvent, DeploymentStatusEvent,
  DownloadEvent, FollowEvent, ForkEvent, ForkApplyEvent, GistEvent, GollumEvent,
  IssueCommentEvent, IssuesEvent, MemberEvent, MembershipEvent, PageBuildEvent, PublicEvent,
  PullRequestEvent, PullRequestReviewCommentEvent, PushEvent, ReleaseEvent, RepositoryEvent,
  StatusEvent, TeamAddEvent, WatchEvent = Value
}

object RefType extends Enumeration {
  type RefType = Value
  val repository, branch, tag = Value
}

object IssueType extends Enumeration {
  type IssueType = Value
  val assigned, unassigned, labeled, unlabeled, opened, closed, reopened = Value
}

object ActionType extends Enumeration {
  type ActionType = Value
  val added, removed = Value
}