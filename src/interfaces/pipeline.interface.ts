import { Observable, Subject } from "rxjs";

export interface Pipeline {
  queue: Subject<string>;
  subjects: Map<string, Subject<any>>;
  cache: Map<string, Observable<any>>;
}
